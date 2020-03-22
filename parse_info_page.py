import asyncio

from utils import init_logger, load_config, init_redis_conn, get_item_info


def main():
    config = load_config('config.yml')

    log_config = config.get('log')
    log_file = log_config.get('parse_item_info_log_file')
    log_level = log_config.get('log_level')
    log_formatter = log_config.get('log_formatter')
    logger = init_logger(log_file, log_level, log_formatter)

    redis_conn = init_redis_conn(config.get('redis'), logger)

    cursor = 0
    batch_count = 100
    item_name = "director"
    item_name_list = ["star", "genre", "director", "studio", "label", "series"]
    for item_name in item_name_list:
        key_list = redis_conn.keys(cursor, "%s_*" % item_name)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(get_item_info(key_list, item_name, redis_conn, logger))
        loop.close()


if __name__ == "__main__":
    main()
