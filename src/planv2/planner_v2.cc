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

#include "planv2/planner_v2.h"
#include <algorithm>
#include <map>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "planv2/ast_node_converter.h"
#include "proto/fe_common.pb.h"

namespace hybridse {
namespace plan {
base::Status SimplePlannerV2::CreateASTScriptPlan(const zetasql::ASTScript *script, PlanNodeList &plan_trees) {  // NOLINT (runtime/references)
    Status status;
    if (nullptr == script) {
        status.msg = "fail to create plan tree: ASTScript is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return status;
    }

    for (auto statement : script->statement_list()) {
        if (!statement->IsSqlStatement()) {
            status.msg = "fail to create plan tree: statement is illegal, sql statement is required";
            status.code = common::kPlanError;
            LOG(WARNING) << status;
            return status;
        }
        switch (statement->node_kind()) {
            case zetasql::AST_QUERY_STATEMENT: {
                const zetasql::ASTQueryStatement *query_statement =
                    statement->GetAsOrNull<zetasql::ASTQueryStatement>();
                CHECK_TRUE(nullptr != query_statement, common::kPlanError,
                           "fail to create plan tree: query statement is illegal")

                PlanNode *query_plan = nullptr;
                CHECK_STATUS(CreateASTQueryPlan(query_statement->query(), &query_plan))

                if (!is_batch_mode_) {
                    // return false if Primary path check fail
                    ::hybridse::node::PlanNode *primary_node;
                    CHECK_STATUS(ValidatePrimaryPath(query_plan, &primary_node))
                    dynamic_cast<node::TablePlanNode *>(primary_node)->SetIsPrimary(true);
                    DLOG(INFO) << "plan after primary check:\n" << *query_plan;
                }

                plan_trees.push_back(query_plan);
                break;
            }
            case zetasql::AST_CREATE_TABLE_STATEMENT: {
                const zetasql::ASTCreateTableStatement *create_statement =
                    statement->GetAsOrNull<zetasql::ASTCreateTableStatement>();
                CHECK_TRUE(nullptr != create_statement, common::kPlanError,
                           "fail to create plan tree: create table statement is illegal")
                PlanNode *create_plan = nullptr;
                CHECK_STATUS(CreateASTCreatetTablePlan(create_statement, &create_plan))
                plan_trees.push_back(create_plan);
                break;
            }
                //            case node::kCreateSpStmt: {
                //                PlanNode *create_sp_plan = nullptr;
                //                PlanNodeList inner_plan_node_list;
                //                const node::CreateSpStmt *create_sp_tree =
                //                    (const node::CreateSpStmt *)parser_tree;
                //                if (CreatePlanTree(create_sp_tree->GetInnerNodeList(),
                //                                   inner_plan_node_list,
                //                                   status) != common::StatusCode::kOk) {
                //                    return status.code;
                //                }
                //                if (!CreateCreateProcedurePlan(parser_tree,
                //                                               inner_plan_node_list,
                //                                               &create_sp_plan, status)) {
                //                    return status.code;
                //                }
                //                plan_trees.push_back(create_sp_plan);
                //                break;
                //            }
            case zetasql::AST_CREATE_DATABASE_STATEMENT:
            case zetasql::AST_DESCRIBE_STATEMENT:
            case zetasql::AST_DROP_STATEMENT:
            case zetasql::AST_SHOW_STATEMENT: {
                node::PlanNode *cmd_plan = nullptr;
                CHECK_STATUS(CreateAstCmdStatementPlan(statement, &cmd_plan));
                plan_trees.push_back(cmd_plan);
                break;
            }
            case zetasql::AST_INSERT_STATEMENT: {
                const zetasql::ASTInsertStatement *insert_statement =
                    statement->GetAsOrNull<zetasql::ASTInsertStatement>();
                CHECK_TRUE(nullptr != insert_statement, common::kPlanError,
                           "fail to create plan tree: insert statement is illegal")
                PlanNode *insert_plan = nullptr;
                CHECK_STATUS(CreateASTInsertPlan(insert_statement, &insert_plan))
                plan_trees.push_back(insert_plan);
                break;
            }
                //            case ::hybridse::node::kFnDef: {
                //                node::PlanNode *fn_plan = nullptr;
                //                if (!CreateFuncDefPlan(parser_tree, &fn_plan, status)) {
                //                    return status.code;
                //                }
                //                plan_trees.push_back(fn_plan);
                //                break;
                //            }
            default: {
                status.msg = "can not handle statement " + statement->GetNodeKindString();
                status.code = common::kPlanError;
                LOG(WARNING) << status;
                return status;
            }
        }
    }
    return base::Status::OK();
}

base::Status SimplePlannerV2::CreateASTQueryPlan(const zetasql::ASTQuery *root, PlanNode **plan_tree) {
    base::Status status;
    if (nullptr == root) {
        status.msg = "can not create query plan node with null query node";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return status;
    }
    node::QueryNode *query_node = nullptr;
    CHECK_STATUS(ConvertQueryNode(root, node_manager_, &query_node))
    DLOG(INFO) << *query_node << std::endl;
    CHECK_STATUS(CreateQueryPlan(query_node, plan_tree))
    return base::Status::OK();
}
base::Status SimplePlannerV2::CreateASTInsertPlan(const zetasql::ASTInsertStatement *root, PlanNode **plan_tree) {
    base::Status status;
    if (nullptr == root) {
        status.msg = "can not create query plan node with null query node";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return status;
    }
    node::InsertStmt *insert_stmt = nullptr;
    CHECK_STATUS(ConvertInsertStatement(root, node_manager_, &insert_stmt))
    DLOG(INFO) << *insert_stmt << std::endl;
    CHECK_STATUS(CreateInsertPlan(insert_stmt, plan_tree))
    return base::Status::OK();
}
base::Status SimplePlannerV2::CreateASTCreatetTablePlan(const zetasql::ASTCreateTableStatement *root,
                                                        PlanNode **plan_tree) {
    base::Status status;
    CHECK_TRUE(nullptr != root, common::kPlanError,
               "can not generate create table plan node with null create statement node")
    node::CreateStmt *create_stmt = nullptr;
    CHECK_STATUS(ConvertCreateTableNode(root, node_manager_, &create_stmt))
    DLOG(INFO) << *create_stmt << std::endl;
    CHECK_STATUS(CreateCreateTablePlan(create_stmt, plan_tree))
    return base::Status::OK();
}
base::Status SimplePlannerV2::CreateAstCmdStatementPlan(const zetasql::ASTStatement *root, PlanNode **plan_tree) {
    base::Status status;
    node::CmdNode *cmd_node = nullptr;
    CHECK_STATUS(ConvertCmdStatement(root, node_manager_, &cmd_node))
    CHECK_TRUE(nullptr != cmd_node, common::kPlanError, "fail to create plan tree: cmd statement node is illegal")
    DLOG(INFO) << *cmd_node << std::endl;
    CHECK_STATUS(CreateCmdPlan(cmd_node, plan_tree))
    return base::Status::OK();
}

}  // namespace plan
}  // namespace hybridse
