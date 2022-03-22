from easydict import EasyDict as edict


DATYS_DATA_DIR = "/app/datys_data/data/"
DATYS_SO_THREADS_DIR = "/app/datys_data/data/so_threads/"
SO_DUMP_DIR = "/app/stackoverflow_dump/"


config = edict()
config.root_path = "/app"
config.data_path = f"{config.root_path}/data"
config.output_path = f"{config.root_path}/output"

# General configuration
config.general = edict()
config.general.remove_previous_logs = False
config.general.logger_config_path = f"{config.root_path}/src/config/logging.conf"
config.general.logger_dest_path = f"{config.root_path}/logs"
config.general.data_path = f"{config.root_path}/data"
config.general.threads_code_path = f"{config.root_path}/threads_code"



config.matcher = edict()
config.matcher.output_path = f"{config.root_path}/output"


config.es = edict()
config.es.host = "localhost"
config.es.port = 8200
config.es.search_result_path = f"{config.output_path}/search_result_wo_parent_acc_ans"
config.es.index_name = "so_java_threads"
config.es.scroll_loop_limit = 100
config.es.scroll_size = 500