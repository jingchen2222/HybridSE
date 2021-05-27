/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_PLAN_PLANNER_H_
#define SRC_PLAN_PLANNER_H_

#include <map>
#include <string>
#include <vector>
#include "base/fe_status.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "node/node_manager.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "proto/fe_type.pb.h"
#include "zetasql/parser/parser.h"
namespace hybridse {
namespace plan {

using base::Status;
using node::NodePointVector;
using node::PlanNode;
using node::PlanNodeList;
using node::SqlNode;
using zetasql::ASTStatementList;

class PlannerV2 {
 public:
    PlannerV2(node::NodeManager *manager, const bool is_batch_mode,
            const bool is_cluster_optimized,
            const bool enable_batch_window_parallelization)
        : is_batch_mode_(is_batch_mode),
          is_cluster_optimized_(is_cluster_optimized),
          enable_window_maxsize_merged_(true),
          enable_batch_window_parallelization_(
              enable_batch_window_parallelization),
          node_manager_(manager) {}
    virtual ~PlannerV2() {}
    virtual int CreatePlanTree(const zetasql::ASTScript* script,
                               PlanNodeList &plan_trees,  // NOLINT
                               Status &status) = 0;           // NOLINT (runtime/references)
    static bool TransformTableDef(
        const std::string &table_name, const NodePointVector &column_desc_list,
        type::TableDef *table,
        Status &status);  // NOLINT (runtime/references)
    bool MergeWindows(const std::map<const node::WindowDefNode *,
                                     node::ProjectListNode *> &map,
                      std::vector<const node::WindowDefNode *> *windows);
    static const std::string GenerateName(const std::string prefix, int id);
 protected:
    const bool is_batch_mode_;
    const bool is_cluster_optimized_;
    const bool enable_window_maxsize_merged_;
    const bool enable_batch_window_parallelization_;
    node::NodeManager *node_manager_;
    bool ExpandCurrentHistoryWindow(
        std::vector<const node::WindowDefNode *> *windows);
    bool IsTable(node::PlanNode *node);
    base::Status CreateTableReferencePlanNode(
        const zetasql::ASTTableExpression *root, node::PlanNode **output);  // NOLINT (runtime/references)
    bool ValidatePrimaryPath(
        node::PlanNode *node, node::PlanNode **output,
        base::Status &status);  // NOLINT (runtime/references)
    bool CreateWindowPlanNode(const node::WindowDefNode *w_ptr,
                              node::WindowPlanNode *plan_node,
                              Status &status);  // NOLINT (runtime/references)
    bool CheckWindowFrame(const node::WindowDefNode *w_ptr,
                          base::Status &status);  // NOLINT (runtime/references)
    std::string MakeTableName(const PlanNode *node) const;
    bool MergeProjectMap(
        const std::map<const node::WindowDefNode *, node::ProjectListNode *>
            &map,
        std::map<const node::WindowDefNode *, node::ProjectListNode *> *output,
        Status &status);  // NOLINT (runtime/references)
};

class SimplePlannerV2 : public PlannerV2 {
 public:
    explicit SimplePlannerV2(node::NodeManager *manager)
        : PlannerV2(manager, true, false, false) {}
    SimplePlannerV2(node::NodeManager *manager, bool is_batch_mode,
                  bool is_cluster_optimized = false,
                  bool enable_batch_window_parallelization = false)
        : PlannerV2(manager, is_batch_mode, is_cluster_optimized,
                  enable_batch_window_parallelization) {}
    int CreatePlanTree(const zetasql::ASTScript* script,
                       PlanNodeList &plan_trees,  // NOLINT
                       Status &status);           // NOLINT (runtime/references)
    base::Status CreateQueryPlan(const zetasql::ASTQuery *root, PlanNode **plan_tree);  // NOLINT (runtime/references)
    base::Status CreateSelectQueryPlan(const zetasql::ASTSelect *root,
                               PlanNode **plan_tree);  // NOLINT (runtime/references)
};

}  // namespace plan
}  // namespace hybridse

#endif  // SRC_PLAN_PLANNER_H_
