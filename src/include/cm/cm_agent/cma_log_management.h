/**
 * @file cma_log_management.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-03
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMA_LOG_MANAGEMENT_H
#define CMA_LOG_MANAGEMENT_H

/* compress buffer size */
#define GZ_BUFFER_LEN 65535
/*
 * trace will be deleted directly if exceeded LOG_GUARD_COUNT
 * the priority of this guard higher than save days but lower than maximum capacity
 */
#define LOG_GUARD_COUNT 20000
#define LOG_GUARD_COUNT_BUF 21000

/* Initialize log pattern and log count when started */
extern LogPattern* logPattern;
extern uint32 len;

int isLogFile(char* fileName);
int get_log_pattern();
void* CompressAndRemoveLogFile(void* arg);
#endif