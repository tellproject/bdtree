#pragma once
#include "bdtree_server_proc.h"
#include <server_config.h>

#ifdef MongoDB_FOUND
DB* create_mongo_db(const std::string &url);
#endif
