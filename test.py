import os
import argparse
from datetime import datetime
import redis
import traceback
from handlers.redis_handler import RedisProcessor


import sys
from pathlib import Path
from handlers import PostgresDBHandler, PostgresTables, CosmosDBHandler, WhisperHandler
from items import STATUS, MODEL, DictionaryData, Dictionary, DICTIONARY_ID, MediaProcessingItem, MediaItem

parser = argparse.ArgumentParser(description="WhisperX job")
# parser.add_argument('--TABLE_NAMES', type=str, required=True, help='Table name')
parser.add_argument('--MODEL_NAME', type=str, required=True, help='Model name from CosmosDB')
parser.add_argument('--COMPUTE_NAME', type=str, required=True, help='Unique compute name')
parser.add_argument('--DB_TYPE', type=str, default='postres', help='Db type')
parser.add_argument('--TEST', type=str, default='False', help='Test mode')
parser.add_argument('--LIMIT', type=int, required=False, help='Define limit to query from DB')
parser.add_argument('--START_DATE', type=str, required=False, help='Define start date to query from DB')
parser.add_argument('--END_DATE', type=str, required=False, help='Define end date to query from DB')
args = parser.parse_args()


TABLE_NAMES = ['telegram_messages']
MODEL_NAME = args.MODEL_NAME
COMPUTE_NAME = args.COMPUTE_NAME
DB_TYPE = args.DB_TYPE
TEST = 1 if args.TEST == 'True' else 0
LIMIT = args.LIMIT
START_DATE = args.START_DATE
END_DATE = args.END_DATE

# Verifying that table name is in PostgresTables
# if TABLE_NAME not in [table.value for table in PostgresTables]:
#     raise Exception(f'Invalid table name: {TABLE_NAME}')


# Verifying that model name is in MODEL
if MODEL_NAME not in [model.value for model in MODEL]:
    raise Exception(f'Invalid model name: {MODEL_NAME}')


redis_db = None
match MODEL_NAME:
    case MODEL.MANUAL.value:
        redis_db = 0 if TEST else 1
        # redis_db = 0
    case MODEL.WHISPERX.value:
        redis_db = 2 if TEST else 3
        # redis_db = 1
    case MODEL.AZURE_SPEACH_TO_TEXT.value:
        redis_db = 4 if TEST else 5
        # redis_db = 2

# redis_client = redis.StrictRedis(host='video-proc-prod.redis.cache.windows.net', port=6379, db=redis_db, password='KaeAnfJa7HnoKwp3xqZPXVKaacplWaaVCAzCaEb6SOY=', ssl=True)
redis_client = redis.StrictRedis(host='video-proc.redis.cache.windows.net', port=6380, db=redis_db, password='on1RY8okeCXK6he8mlrrXeaIxRNlXHLr9AzCaIh2AFk=', ssl=True)

print(f'Starting job pid: {os.getpid()}, redis_db: {redis_db}, model: {MODEL_NAME} in {"TEST" if TEST else "PROD"} mode')


# Get Model from CosmosDB
model: DictionaryData = CosmosDBHandler.get_record_from_dictionary('models', MODEL_NAME)

def redis_key_value(air_date: str, entity: dict, TABLE_NAME: str):
    column_id_name = PostgresDBHandler.get_column_id_name(TABLE_NAME)
    key = f'{model.id}_{TABLE_NAME}_{air_date}_{entity[column_id_name]}_{entity["source_id"]}'
    value = STATUS.PROCESSING.value
    return key, value


def redis_key_value_telegram(entity: dict):
    key = f'telegram_{entity["channel_id"]}_{entity["id"]}'
    value = STATUS.PROCESSING.value
    return key, value


def itteration_step_telegram(is_new: bool, air_date: str, entity: dict, TABLE_NAME: str):
    key, value = redis_key_value_telegram(entity)
    status = redis_client.get(key)
    status = str(status, encoding='utf-8') if status else None
    step = 'CURRENT' if not is_new else 'NEW'
    print(f'{step}: Entity key: {key}, status: {status}')
    if status is None or status == STATUS.PENDING.value or status == STATUS.FAILED.value:
        redis_client.set(key, value)
        print(f'{step}: Entity key: {key}, status : {value} starting...')
        try:
            diarize_result, media = WhisperHandler.handle_telegram(TEST, COMPUTE_NAME, model, TABLE_NAME, entity)
        except Exception as e:
            print(e)
            error = traceback.format_exc()
            print(error)
            redis_client.set(key, STATUS.FAILED.value)
            print(f'{step}: Entity key: {key}, failed, set status: {STATUS.FAILED.value}')
            return



def itteration_step_sensitive():
    sensitive_media_processing = CosmosDBHandler.get_sensitive_media()
    if sensitive_media_processing:
        for media_processing_item in sensitive_media_processing:
            media_processing_item['status'] = 'PROCESSING'
            CosmosDBHandler.upsert_dict('media_processing', media_processing_item)
            updated_media_processing_item = WhisperHandler.handle_sensitive(media_processing_item)
            CosmosDBHandler.upsert_dict('media_processing', updated_media_processing_item)


def itteration_step(is_new: bool, air_date: str, entity: dict, TABLE_NAME: str):
    key, value = redis_key_value(air_date, entity, TABLE_NAME)
    status = redis_client.get(key)
    status = str(status, encoding='utf-8') if status else None
    step = 'CURRENT' if not is_new else 'NEW'
    print(f'{step}: Entity key: {key}, status: {status}')
    # TODO: Uncomment
    if status is None or status == STATUS.PENDING.value or status == STATUS.FAILED.value:
    # if status is None or status == STATUS.PENDING.value:
        redis_client.set(key, value)
        print(f'{step}: Entity key: {key}, status : {value} starting...')
        try:
            diarize_result, media = WhisperHandler.handle(TEST, COMPUTE_NAME, model, TABLE_NAME, entity)
            redis_client.set(key, STATUS.DONE.value)
            print(f'{step}: Entity key: {key}, finished, set status: {STATUS.DONE.value}')
            return diarize_result, media
        except Exception as e:
            print(e)
            if str(e) == 'list index out of range':
                redis_client.set(key, STATUS.DONE.value)
                print(f'{step}: Entity key: {key}, No active speech found in audio, set status: {STATUS.DONE.value}')
                return
            else:
                redis_client.set(key, STATUS.FAILED.value)
                print(f'{step}: Entity key: {key}, failed, set status: {STATUS.FAILED.value}')
                return
    print(f'{step}: Entity key: {key}, status: {status} already in progress, skipping...')
    return

def update_dictionary_entities(dictionary_from_cosmos, dictionary_from_postgres, sources=False):
    existing_ids = {item['id'] for item in dictionary_from_cosmos['data']}

    # Check and add missing dictionaries to dict2['data']
    for item in dictionary_from_postgres['data']:
        if item['id'] not in existing_ids:
            if sources:
                item['postgres_id'] = [item['id']]
            dictionary_from_cosmos['data'].append(item)
    return dictionary_from_cosmos

def update_dictionary():
    authors = PostgresDBHandler.get_authors()
    programs = PostgresDBHandler.get_programs()
    sources = PostgresDBHandler.get_sources()

    authors = [DictionaryData(author['author_id'], author['author_name']) for author in authors]
    authors.insert(0, DictionaryData('0', 'Unknown Author'))
    programs = [DictionaryData(program['program_id'], program['program_name']) for program in programs]
    programs.insert(0, DictionaryData('0', 'Unknown Program'))
    sources = [DictionaryData(source['source_id'], source['source_name']) for source in sources]
    sources.insert(0, DictionaryData('0', 'Unknown Source'))

    authors = Dictionary(DICTIONARY_ID.AUTHORS.value, authors)
    programs = Dictionary(DICTIONARY_ID.PROGRAMS.value, programs)
    sources = Dictionary(DICTIONARY_ID.SOURCES.value, sources)

    authors_cosmos = CosmosDBHandler.get_by_id('dictionary', 'authors')
    programs_cosmos = CosmosDBHandler.get_by_id('dictionary', 'programs')
    sources_cosmos = CosmosDBHandler.get_by_id('dictionary', 'sources')

    authors_dict = update_dictionary_entities(authors_cosmos, authors.to_dict())
    programs_dict = update_dictionary_entities(programs_cosmos, programs.to_dict())
    sources_dict = update_dictionary_entities(sources_cosmos, sources.to_dict())

    container_dict = 'dictionary'
    CosmosDBHandler.upsert_dict(container_dict, authors_dict)
    CosmosDBHandler.upsert_dict(container_dict, programs_dict)
    CosmosDBHandler.upsert_dict(container_dict, sources_dict)


def main_pipeline_telegram():
    # Get list of air dates from PostgresDB
    while True:
        air_dates = PostgresDBHandler.get_air_date_desc('telegram_messages', start_date=START_DATE, end_date=END_DATE, limit=LIMIT)
        # air_date_pivot - it's a date from which we start to get new air dates
        air_date_pivot = datetime.today().strftime('%Y-%m-%d')

        # For statistics
        media_count = 0
        total_air_dates_count = len(air_dates)

        for current_air_date in air_dates:
            # update_dictionary()
            for TABLE_NAME in TABLE_NAMES:
                current_date_ids = PostgresDBHandler.get_ids_of_current_air_day(TABLE_NAME, current_air_date[0])
                new_dates = PostgresDBHandler.get_all_new_air_dates(TABLE_NAME, air_date_pivot, current_date_ids, limit=LIMIT)
                new_air_dates = [date[0] for date in new_dates]
                total_air_dates_count += len(new_air_dates)

                print(f'Current air date: {current_air_date[0]}, air_date_pivot: {air_date_pivot}')
                print(f'Current air date postgres ids len = {len(current_date_ids)} ')
                print(f'New air dates form porsgres len = {len(new_air_dates)}')

                if len(new_air_dates) > 0:
                    for new_air_date in new_air_dates:
                        # processed_ids = CosmosDBHandler.get_postgres_ids_from_media_processing(model.id, new_air_date, TABLE_NAME, test=TEST)
                        processed_ids = []
                        entity_to_process = PostgresDBHandler.get_all_by_air_date(TABLE_NAME, new_air_date, processed_ids, limit=LIMIT, telegram=True)

                        print(f'New Date: Processed postgres ids from CosmosDB = {processed_ids}')
                        print(f'New Date: Entity to process len = {len(entity_to_process)}')

                        for entity in entity_to_process:
                            RedisProcessor.set(f"whisperx_{TABLE_NAME}.last_processed_time", str(datetime.now().timestamp()))
                            itteration_step_telegram(is_new=True, air_date=new_air_date, entity=entity, TABLE_NAME=TABLE_NAME)
                            media_count += 1


                    # Update air_date_pivot
                    print(f'+++++++++ Update air_date_pivot from {air_date_pivot} -> {new_dates[0][1]} +++++++++')
                    air_date_pivot = new_dates[0][1] 

                    print('='*100)
                
                # processed_ids = CosmosDBHandler.get_postgres_ids_from_media_processing(model.id, current_air_date[0], TABLE_NAME, test=TEST)
                processed_ids = []
                entity_to_process = PostgresDBHandler.get_all_by_air_date(TABLE_NAME, current_air_date[0], processed_ids, limit=LIMIT, telegram=True)
                
                print(f'Current Date: Processed postgres ids from CosmosDB  = {processed_ids}')
                print(f'Current Date: Entity to process len = {len(entity_to_process)}')
                for entity in entity_to_process:
                    itteration_step_telegram(is_new=False, air_date=current_air_date[0], entity=entity, TABLE_NAME=TABLE_NAME)
                    media_count += 1
                print('\n')
                continue

        print(f'Total count of processed media: {media_count}')
        print(f'Total count of total_air_dates_count: {total_air_dates_count}')
        print(f'Count of current air dates: {len(air_dates)}')
        print(f'Finished processing dates: {air_dates}')
        if media_count == 0:
            break   


def main_pipline():  
    # Get list of air dates from PostgresDB
    while True:
        air_dates = PostgresDBHandler.get_air_date_desc('videos', start_date=START_DATE, end_date=END_DATE, limit=LIMIT)
        # air_date_pivot - it's a date from which we start to get new air dates
        air_date_pivot = datetime.today().strftime('%Y-%m-%d')

        # For statistics
        media_count = 0
        total_air_dates_count = len(air_dates)

        for current_air_date in air_dates:
            # update_dictionary()
            for TABLE_NAME in TABLE_NAMES:
                current_date_ids = PostgresDBHandler.get_ids_of_current_air_day(TABLE_NAME, current_air_date[0])
                new_dates = PostgresDBHandler.get_all_new_air_dates(TABLE_NAME, air_date_pivot, current_date_ids, limit=LIMIT)
                new_air_dates = [date[0] for date in new_dates]
                total_air_dates_count += len(new_air_dates)

                print(f'Current air date: {current_air_date[0]}, air_date_pivot: {air_date_pivot}')
                print(f'Current air date postgres ids len = {len(current_date_ids)} ')
                print(f'New air dates form porsgres len = {len(new_air_dates)}')

                if len(new_air_dates) > 0:
                    for new_air_date in new_air_dates:
                        processed_ids = CosmosDBHandler.get_postgres_ids_from_media_processing(model.id, new_air_date, TABLE_NAME, test=TEST)
                        processed_ids = []
                        entity_to_process = PostgresDBHandler.get_all_by_air_date(TABLE_NAME, new_air_date, processed_ids, limit=LIMIT)

                        print(f'New Date: Processed postgres ids from CosmosDB = {processed_ids}')
                        print(f'New Date: Entity to process len = {len(entity_to_process)}')

                        for entity in entity_to_process:
                            RedisProcessor.set(f"whisperx_{TABLE_NAME}.last_processed_time", str(datetime.now().timestamp()))
                            itteration_step_sensitive()
                            itteration_step(is_new=True, air_date=new_air_date, entity=entity, TABLE_NAME=TABLE_NAME)
                            media_count += 1


                    # Update air_date_pivot
                    print(f'+++++++++ Update air_date_pivot from {air_date_pivot} -> {new_dates[0][1]} +++++++++')
                    air_date_pivot = new_dates[0][1] 

                    print('='*100)
                
                processed_ids = CosmosDBHandler.get_postgres_ids_from_media_processing(model.id, current_air_date[0], TABLE_NAME, test=TEST)
                processed_ids = []
                entity_to_process = PostgresDBHandler.get_all_by_air_date(TABLE_NAME, current_air_date[0], processed_ids, limit=LIMIT)
                
                print(f'Current Date: Processed postgres ids from CosmosDB  = {processed_ids}')
                print(f'Current Date: Entity to process len = {len(entity_to_process)}')
                for entity in entity_to_process:
                    itteration_step(is_new=False, air_date=current_air_date[0], entity=entity, TABLE_NAME=TABLE_NAME)
                    media_count += 1
                print('\n')
                continue

        print(f'Total count of processed media: {media_count}')
        print(f'Total count of total_air_dates_count: {total_air_dates_count}')
        print(f'Count of current air dates: {len(air_dates)}')
        print(f'Finished processing dates: {air_dates}')
        if media_count == 0:
            break



def process_failed_media():
    MEDIA_CONTAINER = 'media' if TEST == 0 else 'media_test'
    print(f'Processing failed media MEDIA_CONTAINER: {MEDIA_CONTAINER}')
    pending_media_processing_list: list[MediaProcessingItem] = CosmosDBHandler.get_get_media_processing_by_status(STATUS.PENDING.value, test=TEST)
    print(f'Pending media processing count: {len(pending_media_processing_list)}')
    for media_processing in pending_media_processing_list:
        media: MediaItem = CosmosDBHandler.get_by_id(MEDIA_CONTAINER, media_processing.media_id)
        media = MediaItem.from_dict(media)

        entity = PostgresDBHandler.get_by_id(TABLE_NAME, media.postgres_id, media.source_id)
        if entity:
            print(f'Pending media processing: {media.id} would be processed...')
            itteration_step(is_new=False, air_date=media.air_date, entity=entity)



def main():
    main_pipeline_telegram()
    # process_failed_media()


if __name__ == '__main__':
    main()
