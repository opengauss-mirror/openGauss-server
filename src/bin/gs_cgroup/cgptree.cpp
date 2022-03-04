/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 *-------------------------------------------------------------------------
 *
 * cgptree.cpp
 *    Display the Cgroup Tree structure
 *
 * IDENTIFICATION
 *    src/bin/gs_cgroup/cgptree.cpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <pthread.h>
#include <sys/syscall.h>

#include <libcgroup.h>

#include "cgutil.h"

/* data structure of controller information */
struct controller_info {
    char* ctrl_name;               /* controller name */
    char* mount_point;             /* the directory path of mount point */
    struct group_info* group_head; /* point to the head of group info structure */
    struct controller_info* next;  /* point to the next controller */
};

/* data structure of group information */
struct group_info {
    char* grpname;                 /* the group name */
    char* relpath;                 /* the relative path of this group */
    int depth;                     /* the depth of this group */
    int64_t cpu_quota;             /* the value of cpu.cfs_quota_us */
    int64_t cpu_period;            /* the value of cpu.cfs_period_us */
    u_int64_t cpu_shares;          /* the value of cpu.shares */
    u_int64_t blkio_weight;        /* the value of blkio.weight */
    char* blkio_bpsread;           /* io bps read value */
    char* blkio_iopsread;          /* io iops read value */
    char* blkio_bpswrite;          /* io bps write value */
    char* blkio_iopswrite;         /* io iops write value */
    char* cpuset_cpus;             /* cpuset value */
    int64_t cpuset_mems;           /* cpuset memory info */
    int64_t cpuacct_usage;         /* cpu account */
    struct task_info* task_head;   /* the task list in this group */
    struct group_info* parent;     /* point to the parent group (upper level) */
    struct group_info* child_head; /* point to the child group (lower level) */
    struct group_info* prev;       /* point to the previous group (same level) */
    struct group_info* next;       /* point to the next group (same level) */
};

/* data structure of task information */
struct task_info {
    pid_t pid;              /* the thread id (gettid) */
    struct task_info* next; /* point to the next task */
};

extern char* cgutil_subsys_table[];

#ifdef ENABLE_UT
#define static
#endif

#define CHECK_GSCGROUP_TOP_DATABASE                                                        \
    ((strncmp(rel_path, GSCGROUP_TOP_DATABASE ":", sizeof(GSCGROUP_TOP_DATABASE)) == 0) && \
        (0 == strncmp(tmpstr, cgutil_passwd_user->pw_name, tmplen)) &&                     \
        ('\0' == *(tmpstr + tmplen) || '/' == *(tmpstr + tmplen)))

#define CHECK_GSCGROUP_CM                                                 \
    ((strncmp(rel_path, GSCGROUP_CM ":", sizeof(GSCGROUP_CM)) == 0) &&    \
        (0 == strncmp(cm_tmpstr, cgutil_passwd_user->pw_name, tmplen)) && \
        ('\0' == *(cm_tmpstr + tmplen) || '/' == *(cm_tmpstr + tmplen)))

/*
 *****************  STATIC FUNCTIONS ************************
 */

/*
 * function name: cgpstree_free_task_list
 * description  : free task structure list
 * arguments    :
 *       head: the data structure of task list information
 */
static void cgpstree_free_task_list(struct task_info* head)
{
    struct task_info* curr = head;
    struct task_info* next = NULL;

    while (curr != NULL) {
        next = curr->next;

        free(curr);
        curr = NULL;

        if (next == NULL)
            return;

        curr = next;
    }
}

/*
 * function name: cgptree_rec_free_group_tree
 * description  : free group structure recursively
 * arguments    :
 *       current: the data structure of group tree information
 */
static void cgptree_rec_free_group_tree(struct group_info* current)
{
    if (current == NULL)
        return;

    if (current->next != NULL)
        cgptree_rec_free_group_tree(current->next);
    if (current->child_head != NULL)
        cgptree_rec_free_group_tree(current->child_head);

    if (current->grpname != NULL)
        free(current->grpname);

    if (current->relpath != NULL)
        free(current->relpath);

    if (current->blkio_bpsread != NULL)
        free(current->blkio_bpsread);

    if (current->blkio_iopsread != NULL)
        free(current->blkio_iopsread);

    if (current->blkio_bpswrite != NULL)
        free(current->blkio_bpswrite);

    if (current->blkio_iopswrite != NULL)
        free(current->blkio_iopswrite);

    if (current->cpuset_cpus != NULL)
        free(current->cpuset_cpus);

    if (current->task_head != NULL)
        cgpstree_free_task_list(current->task_head);

    free(current);
    current = NULL;

    return;
}

/*
 * function name: cgptree_free_group_tree
 * description  : free group structure of one controller
 * arguments    :
 *          head: the head group of one controller
 */
static void cgptree_free_group_tree(struct group_info* head)
{
    cgptree_rec_free_group_tree(head);

    return;
}

/*
 * function name: cgptree_get_cgroup
 * description  : get the cgroup structure based on the relative path
 * arguments    :
 *       relpath: the relative path
 * return value : the pointer of cgroup structure
 *
 */
static struct cgroup* cgptree_get_cgroup(const char* relpath)
{
    int ret = 0;
    struct cgroup* cg = NULL;

    /* allocate new cgroup structure */
    cg = cgroup_new_cgroup(relpath);
    if (cg == NULL) {
        ret = ECGFAIL;
        fprintf(stdout, "failed to create the new cgroup for %s\n", cgroup_strerror(ret));
        return NULL;
    }

    /* get all information regarding the cgroup from kernel */
    ret = cgroup_get_cgroup(cg);
    if (ret != 0) {
        fprintf(stdout, "failed to get cgroup information for %s(%d)\n", cgroup_strerror(ret), ret);
        cgroup_free(&cg);
        return NULL;
    }

    return cg;
}

/*
 * function name: cgpstree_get_task_list
 * description  : get the task list based on the relative path and mount name
 * arguments    :
 *      rel_path: the relative path of one group
 *           ctl: the name of mount information
 */
static struct task_info* cgpstree_get_task_list(const char* rel_path, const char* ctrl)
{
    void* task_handle = NULL;
    pid_t pid;
    int error;
    struct task_info* tinfo_head = NULL;
    struct task_info* curr_tinfo = NULL;
    struct task_info* prev_tinfo = NULL;

    error = cgroup_get_task_begin(rel_path, ctrl, &task_handle, &pid);

    if (error && error != ECGEOF)
        return NULL;

    while (error != ECGEOF) {
        curr_tinfo = (struct task_info*)calloc(1, sizeof(struct task_info));

        if (curr_tinfo == NULL) {
            cgpstree_free_task_list(tinfo_head);
            cgroup_get_task_end(&task_handle);
            return NULL;
        }

        if (tinfo_head == NULL)
            tinfo_head = curr_tinfo;
        else
            prev_tinfo->next = curr_tinfo;

        curr_tinfo->pid = pid;

        curr_tinfo->next = NULL;

        error = cgroup_get_task_next(&task_handle, &pid);

        if (error && error != ECGEOF) {
            cgpstree_free_task_list(tinfo_head);
            cgroup_get_task_end(&task_handle);
            return NULL;
        }

        prev_tinfo = curr_tinfo;
    }

    cgroup_get_task_end(&task_handle);
    return tinfo_head;
}

/* error report */
#define ERROR_REPORT(error, strings, grpname)                          \
    {                                                                  \
        if (error) {                                                   \
            if (level <= *min_level || (*min_level == 0)) {            \
                fprintf(stdout,                                        \
                    "NOTICE: Cgroup get %s failed for %s group: %s\n", \
                    strings,                                           \
                    grpname,                                           \
                    cgroup_strerror(error));                           \
                *min_level = level;                                    \
            }                                                          \
        }                                                              \
    }

/*
 * function name: cgptree_get_group_info
 * description  : get the group info for specified mount point and group info curr_ginfo
 * arguments    :
 *    curr_ginfo: group info to be updated.
 *    mount_info: specified mount info, like blkio, cpu, cpuset.
 *     min_level: IN@OUT, get the error report min_level
 */
void cgptree_get_group_info(struct group_info* curr_ginfo, const struct cgroup_mount_point& mount_info, int* min_level)
{
    int error;
    int level = 0;

    struct cgroup* cg = NULL;
    struct cgroup_controller* cgc = NULL;

    char* rel_path = curr_ginfo->relpath;

    if (rel_path == NULL)
        return;

    cg = cgptree_get_cgroup(rel_path);

    /* error report has been done in cgptree_get_cgroup */
    if (cg == NULL)
        return;

    level = curr_ginfo->depth;

    /* get controller */
    cgc = cgroup_get_controller(cg, mount_info.name);
    if (cgc == NULL) {
        /*
         * only the lowest levels of wrong case need error report.
         * eg: if Class is not available, then no need to report DefaultClass's
         * error message.
         * when error occurs for Class, the scan of the tree doesn't
         * pause until "Backend" is checked.
         */
        if (*min_level == 0 || (level <= *min_level)) {
            fprintf(stderr,
                "Notice: Cgroup add_controller for group \"%s\" of mount point \"%s\" failed\n",
                curr_ginfo->grpname,
                mount_info.name);

            *min_level = level;
        }

        cgroup_free(&cg);
        cgroup_free_controllers(cg);
        return;
    }

    /* get values of cpu.shares */
    if (0 == strcmp(mount_info.name, MOUNT_CPU_NAME)) {
        error = cgroup_get_value_uint64(cgc, CPU_SHARES, &(curr_ginfo->cpu_shares));
        ERROR_REPORT(error, CPU_SHARES, curr_ginfo->grpname);
    }
    /* get values of cpuset.cpus */
    if (0 == strcmp(mount_info.name, MOUNT_CPUSET_NAME)) {
        error = cgroup_get_value_string(cgc, CPUSET_CPUS, &(curr_ginfo->cpuset_cpus));
        ERROR_REPORT(error, CPUSET_CPUS, curr_ginfo->grpname);
    }
    /* get values of cpuacct.cpus */
    if (0 == strcmp(mount_info.name, MOUNT_CPUACCT_NAME)) {
        error = cgroup_get_value_int64(cgc, CPUACCT_USAGE, &(curr_ginfo->cpuacct_usage));
        ERROR_REPORT(error, CPUACCT_USAGE, curr_ginfo->grpname);
    }

    cgroup_free_controllers(cg);
    cgroup_free(&cg);
    return;
}

/*
 * function name: cgptree_get_group_tree
 * description  : build the tree for the first time, scan the cgroup file.
 * arguments    :
 *    mount_info: specified mount info, like blkio, cpu, cpuset.
 * return       :
 *         ginfo: the root group info of the built group_info tree
 */
static struct group_info* cgptree_get_group_tree(const struct cgroup_mount_point& mount_info)
{
    int curr_depth = -1;
    int prev_depth = -1;
    void* tree_handle = NULL;
    int level = 0;
    int error;
    size_t tmplen = 0;
    char* root_path = NULL;
    char *rel_path = NULL, *tmpstr = NULL, *cm_tmpstr = NULL;
    int min_level = 0;

    struct cgroup_file_info info;
    struct group_info* curr_ginfo = NULL;
    struct group_info* prev_ginfo = NULL;
    struct group_info* root_ginfo = NULL;

    /* begin to walk through the group tree */
    error = cgroup_walk_tree_begin(mount_info.name, "/", 0, &tree_handle, &info, &level);
    if (error && error != ECGEOF)
        return NULL;

    /* save the path of mount point */
    root_path = strdup(info.full_path);
    if (root_path == NULL) {
        cgroup_walk_tree_end(&tree_handle);
        return NULL;
    }

    error = cgroup_walk_tree_set_flags(&tree_handle, CGROUP_WALK_TYPE_PRE_DIR);
    if (error) {
        free(root_path);
        root_path = NULL;
        cgroup_walk_tree_end(&tree_handle);
        return NULL;
    }

    while (error != ECGEOF) {
        /* get the relative path */
        rel_path = (char*)(info.full_path + strlen(root_path));

        tmpstr = rel_path + sizeof(GSCGROUP_TOP_DATABASE);
        cm_tmpstr = rel_path + sizeof(GSCGROUP_CM);
        tmplen = strlen(cgutil_passwd_user->pw_name);

        if ((CHECK_GSCGROUP_TOP_DATABASE || CHECK_GSCGROUP_CM) && info.type == CGROUP_FILE_TYPE_DIR) {
            curr_ginfo = (struct group_info*)calloc(1, sizeof(struct group_info));

            if (curr_ginfo == NULL)
                goto error;

            curr_ginfo->depth = info.depth;
            curr_ginfo->grpname = strdup(info.path);

            if (curr_ginfo->grpname == NULL) {
                free(curr_ginfo);
                curr_ginfo = NULL;
                goto error;
            }

            curr_ginfo->relpath = strdup(rel_path);

            if (curr_ginfo->relpath == NULL) {
                free(curr_ginfo->grpname);
                curr_ginfo->grpname = NULL;
                free(curr_ginfo);
                curr_ginfo = NULL;
                goto error;
            }

            /* get the task list in this group */
            curr_ginfo->task_head = cgpstree_get_task_list(rel_path, mount_info.name);

            cgptree_get_group_info(curr_ginfo, mount_info, &min_level);

            curr_depth = info.depth;

            if (root_ginfo == NULL) {
                root_ginfo = curr_ginfo;
            } else if (prev_depth == curr_depth) {
                prev_ginfo->next = curr_ginfo;
                curr_ginfo->prev = prev_ginfo;
                curr_ginfo->parent = prev_ginfo->parent;
            } else if ((prev_depth + 1) == curr_depth) {
                prev_ginfo->child_head = curr_ginfo;
                curr_ginfo->parent = prev_ginfo;
            } else { /* must jump when if current is for prev neither child nor sibling */
                while (true) {
                    if (curr_ginfo->depth == prev_ginfo->depth)
                        break;
                    prev_ginfo = prev_ginfo->parent;
                    continue;
                }

                /** prev_ginfo is sibling here to follow **/
                prev_ginfo->next = curr_ginfo;
                curr_ginfo->prev = curr_ginfo;
                curr_ginfo->parent = prev_ginfo->parent;
            }

            prev_ginfo = curr_ginfo;
            prev_depth = prev_ginfo->depth;
        }

        error = cgroup_walk_tree_next(0, &tree_handle, &info, level);
        if (error && error != ECGEOF) {
            /* free resource when error */
            goto error;
        }
    }

    free(root_path);
    root_path = NULL;
    cgroup_walk_tree_end(&tree_handle);

    return root_ginfo;

error:
    free(root_path);
    root_path = NULL;
    cgroup_walk_tree_end(&tree_handle);

    if (root_ginfo != NULL)
        cgptree_free_group_tree(root_ginfo);

    return NULL;
}

/*
 * function name: cgptree_walk_group
 * description  : get cgroup info with linked list.
 */
static struct group_info* cgptree_walk_group(struct group_info* ginfo)
{
    if (ginfo->child_head != NULL)
        return ginfo->child_head;

    if (ginfo->next != NULL)
        return ginfo->next;

    while (ginfo->parent != NULL) {
        ginfo = ginfo->parent;

        if (ginfo->next != NULL)
            return ginfo->next;
    }
    return NULL;
}

/*
 * function name: cgptree_get_tree_info
 * description  : scan the tree, and get the information of the mount_info
 *              : from the cgroup file system
 * arguments    :
 *    mount_info: specified mount info, like blkio, cpu, cpuset.
 * return       :
 *    root_ginfo: the root group info of the built group_info tree
 */
struct group_info* cgptree_get_tree_info(const cgroup_mount_point& mount_info, struct group_info* root_ginfo)
{
    struct group_info* curr_ginfo = root_ginfo;
    int min_level = 0;

    while (curr_ginfo != NULL) {
        char* rel_path = curr_ginfo->relpath;

        if (rel_path == NULL) {
            curr_ginfo = cgptree_walk_group(curr_ginfo);
            continue;
        }

        /* get the task list in this group */
        cgptree_get_group_info(curr_ginfo, mount_info, &min_level);

        curr_ginfo = cgptree_walk_group(curr_ginfo);
    }

    return root_ginfo;
}

/*
 * function name: cgptree_get_cgroup_new
 * description  : scan the mount controller, and get the group_info of the tree,
 *              : fill the different subsys information
 *              : in the same group_info tree
 * return       :
 *         ginfo: the root group info of the fully built group_info tree
 */
static struct group_info* cgptree_get_cgroup_new()
{
    int error = 0;
    void* ctrl_handle = NULL;
    int count = 0;

    struct cgroup_mount_point info = {{0}, {0}};
    struct group_info* ginfo = NULL;

    error = cgroup_get_controller_begin(&ctrl_handle, &info);
    if (error) {
        fprintf(stderr, "get controller begin failed: %s\n", cgroup_strerror(error));
        return NULL;
    }

    while (error != ECGEOF) {

        if (*info.name && strcmp(info.name, MOUNT_CPU_NAME) != 0 && strcmp(info.name, MOUNT_CPUSET_NAME) != 0 &&
            strcmp(info.name, MOUNT_CPUACCT_NAME) != 0) {
            error = cgroup_get_controller_next(&ctrl_handle, &info);
            if (error && error != ECGEOF) {
                fprintf(stderr, "get next controller failed: %s\n", cgroup_strerror(error));
                break;
            }
            continue;
        }

        /* for new_allocate tree, get the tree_node info from the first scanned subsys mount_info */
        if (!count) {
            ginfo = cgptree_get_group_tree(info);
        } else {
            /* the following subsys group info will be filled in the already built tree */
            ginfo = cgptree_get_tree_info(info, ginfo);
        }

        /*
         * get the group tree of this controller
         * if the group is not valid, skip it
         */
        if (ginfo == NULL) {
            fprintf(stderr, "Notice: get tree information for mount point %s failed\n", info.name);

            error = cgroup_get_controller_next(&ctrl_handle, &info);
            if (error && error != ECGEOF) {
                fprintf(stderr, "get next controller failed: %s\n", cgroup_strerror(error));
                break;
            }
            continue;
        }

        /*
         * the first time allocate memory to build the tree
         * next loop, there will be no need to build any node of the tree,
         * but only fill in the node info of the tree.
         */
        count++;

        error = cgroup_get_controller_next(&ctrl_handle, &info);
        if (error && error != ECGEOF) {
            fprintf(stderr, "get next controller failed: %s\n", cgroup_strerror(error));
            break;
        }
    }

    (void)cgroup_get_controller_end(&ctrl_handle);

    return ginfo;
}
/*
 * function name: cgptree_print_space
 * description  : print the space based on level
 *
 */
static void cgptree_print_space(int level)
{
    while (level-- > 0)
        printf("\t");
}

/*
 * function name: cgptree_delete_group
 * description  : delete the cgroup recursively
 */
static void cgptree_delete_group(struct group_info* ginfo)
{
    if (NULL == ginfo)
        return;

    if (ginfo->child_head != NULL)
        cgptree_delete_group(ginfo->child_head);

    if (ginfo->next != NULL)
        cgptree_delete_group(ginfo->next);

    cgexec_delete_cgroups(ginfo->relpath);
}

/*
 * function name: cgptree_print_group
 * description  : print the group information as tree style
 * arguments    :
 *      ctlrname: the name of the controller
 *         ginfo: the head of group information
 *         level: the level of the group
 *
 */
#define BLKIO_STR_UPDATE(s)      \
    {                            \
        char* q = NULL;          \
        do {                     \
            q = strchr(s, '\n'); \
            if (q != NULL)       \
                *q = ';';        \
        } while (q != NULL);     \
    }

/*
 * @Description: print cgroup info in tree.
 * @IN ginfo: group info
 * @IN level: group level
 * @Return:  void
 * @See also:
 */
static void cgptree_print_group_new(struct group_info* ginfo, int level)
{
    struct task_info* tinfo = ginfo->task_head;
    int cnt = 0;

    cgptree_print_space(level);

    /* print group name */
    fprintf(stdout, "- %s ", ginfo->grpname);

    /* print cpu shares */
    fprintf(stdout, "(shares: %lu,", ginfo->cpu_shares);

    /* print cpuset */
    fprintf(stdout, " cpus: %s", ginfo->cpuset_cpus);

    if (cgutil_is_sles11_sp2 || cgexec_check_SLESSP2_version()) {
        fprintf(stdout, ", weight: %lu", ginfo->blkio_weight);

        if ((ginfo->blkio_bpsread != NULL) && ginfo->blkio_bpsread[0]) {
            BLKIO_STR_UPDATE(ginfo->blkio_bpsread);
            fprintf(stdout, ", bpsread: \"%s\"", ginfo->blkio_bpsread);
        }
        if ((ginfo->blkio_iopsread != NULL) && ginfo->blkio_iopsread[0]) {
            BLKIO_STR_UPDATE(ginfo->blkio_iopsread);
            fprintf(stdout, ", iopsread: \"%s\"", ginfo->blkio_iopsread);
        }
        if ((ginfo->blkio_bpswrite != NULL) && ginfo->blkio_bpswrite[0]) {
            BLKIO_STR_UPDATE(ginfo->blkio_bpswrite);
            fprintf(stdout, ", bpswrite: \"%s\"", ginfo->blkio_bpswrite);
        }
        if ((ginfo->blkio_iopswrite != NULL) && ginfo->blkio_iopswrite[0]) {
            BLKIO_STR_UPDATE(ginfo->blkio_iopswrite);
            fprintf(stdout, ", iopswrite: \"%s\"", ginfo->blkio_iopswrite);
        }
    }

    fprintf(stdout, ")\n");

    if (tinfo != NULL)
        cgptree_print_space(level + 1);

    /* print thread id in the group */
    while (tinfo != NULL) {
        fprintf(stdout, "%8d ", (int)tinfo->pid);

        if (0 == (++cnt) % 5) {
            fprintf(stdout, "\n");
            cgptree_print_space(level + 1);
        }

        tinfo = tinfo->next;
    }

    if (cnt)
        fprintf(stdout, "\n");

    fflush(stdout);

    /* print next one */
    if (ginfo->child_head != NULL)
        cgptree_print_group_new(ginfo->child_head, level + 1);

    if (ginfo->next != NULL)
        cgptree_print_group_new(ginfo->next, level);
}

/*
 * function name: cgptree_free
 * description  : free the controller information
 * arguments    :
 *    cinfo_head: the head of controller information
 *
 */
static void cgptree_free(struct controller_info* cinfo_head)
{
    struct controller_info* curr = cinfo_head;
    struct controller_info* next = NULL;

    if (NULL == curr)
        return;

    /* free controller data structure */
    while (curr != NULL) {
        next = curr->next;

        if (curr->ctrl_name != NULL)
            free(curr->ctrl_name);
        if (curr->mount_point != NULL)
            free(curr->mount_point);

        cgptree_free_group_tree(curr->group_head);

        free(curr);
        curr = NULL;
        curr = next;
    }
}

void free_controller_list_resource(struct controller_info* curr_cinfo,
                                   char* curr_path,
                                   struct controller_info* cinfo_head,
                                   void* ctrl_handle)
{
    if (curr_cinfo != NULL) {
        if (curr_cinfo->ctrl_name != NULL) {
            free(curr_cinfo->ctrl_name);
        }
        free(curr_cinfo);
    }
    if (curr_path != NULL) {
        free(curr_path);
    }
    cgptree_free(cinfo_head);
    cgroup_get_controller_end(&ctrl_handle);
}

/*
 * function name: cgptree_get_controller_list
 * description  : read all cgroups and make up of the controller information
 * return value : the head of controller information
 *
 */
static struct controller_info* cgptree_get_controller_list(void)
{
    int error = 0;
    char* curr_path = NULL;
    void* ctrl_handle = NULL;
    struct cgroup_mount_point info = {{0}, {0}};
    struct controller_info* cinfo_head = NULL;
    struct controller_info* curr_cinfo = NULL;
    struct controller_info* prev_cinfo = NULL;
    struct group_info* ginfo = NULL;

    error = cgroup_get_controller_begin(&ctrl_handle, &info);
    if (error) {
        fprintf(stderr, "get controller begin failed: %s\n", cgroup_strerror(error));
        return NULL;
    }

    while (error != ECGEOF) {
        curr_path = strdup(info.path);
        if (curr_path == NULL) {
            free_controller_list_resource(curr_cinfo, curr_path, cinfo_head, ctrl_handle);
            return NULL;
        }

        /*
         * get the group tree of this controller
         * if the group is not valid, skip it
         */
        ginfo = cgptree_get_group_tree(info);
        if (NULL == ginfo) {
            free(curr_path);

            error = cgroup_get_controller_next(&ctrl_handle, &info);
            if (error && error != ECGEOF) {
                fprintf(stderr, "get next controller failed: %s\n", cgroup_strerror(error));
                break;
            }

            continue;
        }

        /* allocate structure for new controller */
        curr_cinfo = (struct controller_info*)calloc(1, sizeof(struct controller_info));
        if (curr_cinfo == NULL) {
            free_controller_list_resource(curr_cinfo, curr_path, cinfo_head, ctrl_handle);
            return NULL;
        }

        curr_cinfo->ctrl_name = strdup(info.name);
        if (curr_cinfo->ctrl_name == NULL) {
            free_controller_list_resource(curr_cinfo, curr_path, cinfo_head, ctrl_handle);
            return NULL;
        }
        curr_cinfo->mount_point = strdup(info.path);
        if (curr_cinfo->mount_point == NULL) {
            free_controller_list_resource(curr_cinfo, curr_path, cinfo_head, ctrl_handle);
            return NULL;
        }

        /* get the group tree of this controller */
        curr_cinfo->group_head = ginfo;

        error = cgroup_get_controller_next(&ctrl_handle, &info);
        if (error && error != ECGEOF) {
            fprintf(stderr, "get next controller failed: %s\n", cgroup_strerror(error));
            if (curr_cinfo->ctrl_name != NULL)
                free(curr_cinfo->ctrl_name);
            if (curr_cinfo->mount_point != NULL)
                free(curr_cinfo->mount_point);
            cgptree_free_group_tree(curr_cinfo->group_head);
            free(curr_cinfo);
            break;
        }

        if (cinfo_head == NULL)
            cinfo_head = curr_cinfo;
        else
            prev_cinfo->next = curr_cinfo;

        prev_cinfo = curr_cinfo;
        if (curr_path != NULL) {
            free(curr_path);
        }
        curr_cinfo = NULL;
    }

    cgroup_get_controller_end(&ctrl_handle);

    return cinfo_head;
}
/*
 **************** EXTERNAL FUNCTION ********************************
 */

/*
 * function name: cgptree_display_cgroups
 * description  : display the Cgroup tree information
 * return value :
 *            -1: abnormal
 *             0: normal
 */
int cgptree_display_cgroups(void)
{
    struct group_info* ginfo_new = NULL;

    ginfo_new = cgptree_get_cgroup_new();
    if (ginfo_new == NULL) {
        /* release group info */
        fprintf(stderr, "failed to get the new cgroup tree information!\n");
        return -1;
    }

    /* print current all mount points */
    for (int i = 0; i < MOUNT_SUBSYS_KINDS; ++i) {
        if (i == MOUNT_BLKIO_ID)
            continue;

        if (i == 0)
            fprintf(stdout, "Mount Information:\n");

        fprintf(stdout, "%s:%s\n", cgutil_subsys_table[i], cgutil_opt.mpoints[i]);
    }

    fprintf(stdout, "\nGroup Tree Information:\n");

    /* print group info */
    cgptree_print_group_new(ginfo_new, 0);

    cgptree_rec_free_group_tree(ginfo_new);

    return 0;
}
/*
 * function name: cgptree_drop_cgroups
 * description  : drop all users' cgroup
 * return value :
 *            -1: abnormal
 *             0: normal
 */
int cgptree_drop_cgroups(void)
{
    struct controller_info* cinfo_head = NULL;

    cinfo_head = cgptree_get_controller_list();
    if (cinfo_head == NULL) {
        fprintf(stderr, "failed to get Cgroup tree information!\n");
        return -1;
    }

    cgptree_delete_group(cinfo_head->group_head);

    cgptree_free(cinfo_head);

    return 0;
}

/*
 * function name: cgptree_drop_nodegroup_cgroups
 * description  : drop all users' nodegroup cgroup
 * return value :
 *            -1: abnormal
 *             0: normal
 */
int cgptree_drop_nodegroup_cgroups(const char* name)
{
    struct controller_info* cinfo_head = NULL;

    cinfo_head = cgptree_get_controller_list();
    if (cinfo_head == NULL) {
        fprintf(stderr, "failed to get Cgroup tree information!\n");
        return -1;
    }

    struct group_info* ginfo = cinfo_head->group_head->child_head;
    while (ginfo != NULL) {
        if (strcmp(ginfo->grpname, name) == 0) {
            break;
        }

        ginfo = ginfo->next;
    }

    if (ginfo != NULL) {
        if (ginfo->child_head != NULL)
            cgptree_delete_group(ginfo->child_head);

        cgexec_delete_cgroups(ginfo->relpath);
    } else if (cinfo_head->group_head->child_head == NULL) {
        fprintf(stderr,
            "failed to find the cgroup (%s) with "
            " controller name(%s), mount_point(%s), group_head(%s).\n",
            name,
            cinfo_head->ctrl_name,
            cinfo_head->mount_point,
            cinfo_head->group_head->grpname);
    } else {
        fprintf(stderr,
            "failed to find the cgroup (%s) with "
            " controller name(%s), mount_point(%s), "
            " group_head(%s), child_head(%s).\n",
            name,
            cinfo_head->ctrl_name,
            cinfo_head->mount_point,
            cinfo_head->group_head->grpname,
            cinfo_head->group_head->child_head->grpname);
    }

    cgptree_free(cinfo_head);

    return 0;
}
