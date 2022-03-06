/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 * 
 * be_module.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/be_module.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_INCLUDE_UTILS_BE_MODULE_H
#define SRC_INCLUDE_UTILS_BE_MODULE_H

/*
 * How to add your module id ?
 * 1. add your module id before MOD_MAX in ModuleId;
 * 2. fill up module_map[] about module name, and keep their ordering;
 *
 */
enum ModuleId {
    /* fastpath for all modules on/off */
    MOD_ALL = 0,
    /* add your module id following */

    MOD_COMMAND,      /* Commands */
    MOD_DFS,          /* DFS */
    MOD_GUC,          /* GUC */
    MOD_GSCLEAN,      /* gs_clean */
    MOD_HDFS,         /* HDFS feature */
    MOD_ORC,          /* ORC storage format */
    MOD_SLRU,         /* Simple LRU buffering manager */
    MOD_MEM,          /* Memory Manager */
    MOD_AUTOVAC,      /* auto-vacuum */
    MOD_CACHE,        /* cache manager  include data cache(cu , dfs) ,index cache(dfs)*/
    MOD_ADIO,         /* ADIO  feature */
    MOD_SSL,          /* SSL */
    MOD_GDS,          /* GDS */
    MOD_TBLSPC,       /* Tablespace */
    MOD_WLM,          /* workload manager*/
    MOD_OBS,          /* OBS */
    MOD_INDEX,        /* index */
    MOD_EXECUTOR,     /* Row Executor */
    MOD_OPFUSION,     /* Bypass Opfusion */
    MOD_GPC,          /* Global plancache */
    MOD_GSC,          /* Global syscache */
    MOD_VEC_EXECUTOR, /* Vector Executor */
    MOD_STREAM,       /* Stream */
    MOD_LLVM,         /* LLVM */
    MOD_OPT,          /* Optimizer default module */
    MOD_OPT_REWRITE,  /* Optimizer sub-module:rewrite */
    MOD_OPT_JOIN,     /* Optimizer sub-module:join */
    MOD_OPT_AGG,      /* Optimizer sub-module:agg */
    MOD_OPT_CHOICE,   /* Optimizer sub-module: choice of gplan or cplan */
    MOD_OPT_SUBPLAN,  /* Optimizer sub-module:subplan */
    MOD_OPT_SETOP,    /* Optimizer sub-module:setop */
    MOD_OPT_SKEW,     /* Optimizer sub-module:data skew */
    MOD_OPT_PLANNER,  /* Optimizer sub-module:planner */
    MOD_UDF,          /* fenced udf */
    MOD_COOP_ANALYZE, /* cooperation analyze */
    MOD_WLM_CP,       /* wlm for the comupte pool */
    MOD_ACCELERATE,   /* accelerate with comptue pool */
    MOD_MOT,          /* MOT */
    MOD_PLANHINT,     /* plan hint */
    MOD_PARQUET,      /* Parquet storage format */
    MOD_PGSTAT,       /* pgstat */
    MOD_CARBONDATA,   /* Carbondata storage format */

    /* MODULE FOR TRANSACTION LOG CONTROL , USE LOG LEVEL*/
    MOD_TRANS_SNAPSHOT, /* Snapshot */
    MOD_TRANS_XACT,     /* Xact Finite-State-Machine(FSM) */
    MOD_TRANS_HANDLE,   /* Handle for Transaction */
    MOD_TRANS_CLOG,     /* Clog Write */

    MOD_EC,         /* Extension Connector */
    MOD_REMOTE,     /* remote read */
    MOD_CN_RETRY,   /* cn retry */
    MOD_PLSQL,      /* plpgsql */
    MOD_TS,         /* TEXT SEARCH */
    MOD_SEQ,        /* sequence */
    MOD_REDO,       /* redo log */
    MOD_FUNCTION,   /* internal function */
    MOD_PARSER,     /* parser module*/
    MOD_INSTR,      /* Instrumentation */
    MOD_WDR_SNAPSHOT,  /* wdr snapshot */
    MOD_INCRE_CKPT, /* incremental checkpoint */
    MOD_INCRE_BG,   /* incremental checkpoint bgwriter */
    MOD_DW,         /* double write */
    MOD_RTO_RPO,    /* log control */
    MOD_HEARTBEAT,  /* heartbeat */
    MOD_COMM_IPC,   /* comm ipc performance */
    MOD_COMM_PARAM, /* comm session params */
    MOD_TIMESERIES, /* timeseries feature */
    MOD_SCHEMA,     /* schema search */
    
    MOD_SEGMENT_PAGE,  /* segment page storage */
    MOD_LIGHTPROXY, /* lightProxy */
    MOD_HOTKEY,     /* hotkey */
    MOD_THREAD_POOL,  /* thread_pool */
    MOD_OPT_AI,     /* ai optimizer */
    MOD_WALRECEIVER,  /* walreceiver */
    MOD_USTORE,     /* ustore */
    MOD_UNDO,       /* undo */
    MOD_GEN_COL,   /* generated column */
    MOD_DCF,        /* DCF paxos */
    MOD_DB4AI,      /* DB4AI & AUTOML */
    MOD_PLDEBUGGER,
    MOD_ADVISOR,    /* sql advisor */

    MOD_SEC,           /* Security default module */
    MOD_SEC_FE,        /* Security sub-module: full encryption */
    MOD_SEC_LEGER,     /* Security sub-module: ledger database */
    MOD_SEC_POLICY,    /* Security sub-module: masking, auditing and RLS policies */
    MOD_SEC_SDD,       /* Security sub-module: sensitive data discovery */
    MOD_SEC_TDE,       /* Security sub-module: transparent data encryption */

    MOD_COMM_PROXY,    /* for cbb comm_proxy */
    MOD_COMM_POOLER,   /* for pooler communication */
    MOD_VACUUM,     /* lazy vacuum */
    MOD_JOB,        /* job/scheduler job related */
    MOD_SPI,
    MOD_NEST_COMPILE,
    MOD_RESOWNER,
    MOD_LOGICAL_DECODE,    /* logical decode */
    MOD_GPRC, /* global package runtime cache */

    /*
     * Add your module id above.
     * Do not forget to fill up module_map[] about module name, and keep their ordering;
     */
    MOD_MAX
};

/* 1 bit <--> 1 module, including MOD_MAX. its size is
 *      ((MOD_MAX+1)+7)/8 = MOD_MAX/8 + 1
 */
#define BEMD_BITMAP_SIZE (1 + (MOD_MAX / 8))

#define MODULE_ID_IS_VALID(_id) ((_id) >= MOD_ALL && (_id) < MOD_MAX)
#define ALL_MODULES(_id) (MOD_ALL == (_id))

/* Is it a valid and signle module id ? */
#define VALID_SINGLE_MODULE(_id) ((_id) > MOD_ALL && (_id) < MOD_MAX)

/* max length of module name */
#define MODULE_NAME_MAXLEN (16)

/* delimiter of module name list about GUC parameter */
#define MOD_DELIMITER ','

/* map about module id and its name */
typedef struct module_data {
    ModuleId mod_id;
    const char mod_name[MODULE_NAME_MAXLEN];
} module_data;

/*******************  be-module id <--> name **********************/
extern ModuleId get_module_id(const char* module_name);

/* Notice:
 *   declaration here only for the following inline functions,
 *   never use it within the other files directly.
 */
extern const module_data module_map[];

/*
 * @Description: get the default module name. normally
 *   this module is not defined in ModuleId.
 * @Return: default module name
 * @See also:
 */
inline const char* get_default_module_name(void)
{
    return module_map[MOD_MAX].mod_name;
}

/*
 * @Description: find a module's name according to its id.
 * @IN module_id: module id
 * @Return: module name
 * @See also:
 */
inline const char* get_valid_module_name(ModuleId module_id)
{
    return module_map[module_id].mod_name;
}

/*******************  be-module logging **********************/
extern bool check_module_name_unique(void);
extern void module_logging_batch_set(ModuleId* mods, int nmods, bool turn_on, bool apply_all_modules);
extern bool module_logging_is_on(ModuleId module_id);
extern void module_logging_enable_comm(ModuleId module_id);

#endif /* SRC_INCLUDE_UTILS_BE_MODULE_H */
