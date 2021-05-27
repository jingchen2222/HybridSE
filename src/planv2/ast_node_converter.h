/*
 * ast_node_converter.h
 * Copyright (C) 4paradigm 2021 chenjing <chenjing@4paradigm.com>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef HYBRIDSE_AST_NODE_CONVERTER_H
#define HYBRIDSE_AST_NODE_CONVERTER_H
#include "node/node_manager.h"
#include "udf/udf.h"
#include "zetasql/parser/parser.h"

namespace hybridse {
namespace plan {
base::Status ConvertExprNode(const zetasql::ASTExpression* ast_expression, node::NodeManager* node_manager,
                             node::ExprNode** output);
base::Status ConvertOrderBy(const zetasql::ASTOrderBy* order_by, node::NodeManager* node_manager,
                            node::OrderByNode** output);

base::Status ConvertExprNodeList(const absl::Span<const zetasql::ASTExpression* const>& expression_list,
                                 node::NodeManager* node_manager, node::ExprListNode** output);
base::Status ConvertFrameBound(const zetasql::ASTWindowFrameExpr* window_frame_expr, node::NodeManager* node_manager,
                               node::FrameBound** output);
base::Status ConvertFrameNode(const zetasql::ASTWindowFrame* window_frame, node::NodeManager* node_manager,
                              node::FrameNode** output);
base::Status ConvertWindowDefinition(const zetasql::ASTWindowDefinition* window_definition,
                                     node::NodeManager* node_manager, node::WindowDefNode** output);
base::Status ConvertWindowSpecification(const zetasql::ASTWindowSpecification* window_spec,
                                     node::NodeManager* node_manager, node::WindowDefNode** output);

}  // namespace plan
}  // namespace hybridse
#endif  // HYBRIDSE_AST_NODE_CONVERTER_H
