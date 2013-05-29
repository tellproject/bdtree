#pragma once
#include "bdtree_server_proc.h"
#include "bdtree.h"

DB* create_bdtree_db(bdtree::logical_table_cache<std::string, bdtree::empty_t> &cache, uint64_t data);
