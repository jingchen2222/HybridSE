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
#include "zetasql/parser/parser.h"

namespace hybridse {
namespace plan {

base::Status ConvertExprNodeList(const absl::Span<const ASTExpression* const>& expression_list,
                                 node::NodeManager* node_manager, node::ExprNodeList** output) {
    auto expr_list = node_manager->MakeExprList();
    for (auto expression : expression_list) {
        expr_list->AddChild(ConvertExpr(expression, node_manager));
    }
    return expr_list;
}
base::Status ConvertOrderBy(const zetasql::ASTOrderBy* order_by, node::NodeManager* node_manager,
                            node::OrderByNode** output) {
    node::ExprListNode* ordering_expressions = nullptr;
    CHECK_STATUS(ConvertExprNodeList(order_by->ordering_expressions(), node_manager, &ordering_expressions))

    *output = node_manager->MakeOrderByNode(ordering_expressions, order_by->ordering_expressions())
}
base::Status ConvertOp(const zetasql::ASTBinaryExpression::Op& op, node::FnOperator* output) {
    base::Status status;
    switch (op) {
        case zetasql::ASTBinaryExpression::Op::EQ: {
            *output = node::FnOperator::kFnOpEq;
            break;
        }
        case zetasql::ASTBinaryExpression::Op::NE:
        case zetasql::ASTBinaryExpression::Op::NE2: {
            *output = node::FnOperator::kFnOpNeq;
            break;
        }
        case zetasql::ASTBinaryExpression::Op::GT: {
            *output = node::FnOperator::kFnOpGt;
            break;
        }
        case zetasql::ASTBinaryExpression::Op::LT: {
            *output = node::FnOperator::kFnOpLt;
            break;
        }
        case zetasql::ASTBinaryExpression::Op::GE: {
            *output = node::FnOperator::kFnOpGE;
            break;
        }
        case zetasql::ASTBinaryExpression::Op::LE: {
            *output = node::FnOperator::kFnOpLE;
            break;
        }
        case zetasql::ASTBinaryExpression::Op::PLUS: {
            *output = node::FnOperator::kFnOpAdd;
            break;
        }
        case zetasql::ASTBinaryExpression::Op::MINUS: {
            *output = node::FnOperator::kFnOpMinus;
            break;
        }
        case zetasql::ASTBinaryExpression::Op::MULTIPLY: {
            *output = node::FnOperator::kFnOpMulti;
            break;
        }
        case zetasql::ASTBinaryExpression::Op::DIVIDE: {
            *output = node::FnOperator::kFnOpFDiv;
            break;
        }
        case zetasql::ASTBinaryExpression::Op::LIKE: {
            *output = node::FnOperator::kFnOpLike;
            break;
        }
        default: {
            status.msg = "Unsupport operator: " + zetasql::ASTBinaryExpression::op();
            status.code = common::kSqlError;
            *output = node::FnOperator::kFnOpNone;
            return status;
        }
    }
    return status;
}
base::Status ConvertExprNode(const zetasql::ASTExpression* ast_expression, node::NodeManager* node_manager,
                             node::ExprNode** output) {
    if (nullptr == ast_expression) {
        return nullptr;
    }
    base::Status status;
    switch (ast_expression->node_kind()) {
        case zetasql::AST_IDENTIFIER: {
            output = node_manager->MakeExprIdNode(ast_expression->GetAsOrDie<zetasql::ASTIdentifier>()->GetAsString());
            break;
        }
        case zetasql::AST_PATH_EXPRESSION: {
            auto* path_expression = ast_expression->GetAsOrDie<zetasql::ASTPathExpression>();
            output = node_manager->MakeColumnRefNode(path_expression->last_name(), path_expression->first_name());
            break;
        }
        case zetasql::AST_BINARY_EXPRESSION: {
            auto* binary_expression = ast_expression->GetAsOrDie<zetasql::ASTBinaryExpression>();
            node::ExprNode* lhs = nullptr;
            node::ExprNode** rhs = nullptr;
            node::FnOperator op;

            CHECK_STATUS(ConvertExprNode(binary_expression->lhs(), node_manager, &lhs))
            CHECK_STATUS(ConvertExprNode(binary_expression->rhs(), node_manager, &rhs))
            CHECK_STATUS(ConvertOp(binary_expression->op(), &op))
            *output = node_manager->MakeBinaryExprNode(lhs, rhs, op, node_manager, status);
            break;
        }
        case zetasql::AST_UNARY_EXPRESSION: {
            auto* unary_expression = ast_expression->GetAsOrDie<zetasql::ASTUnaryExpression>();
            node::ExprNode* operand = nullptr;
            CHECK_STATUS(ConvertExprNode(unary_expression->operand(), node_manager, &operand))
            CHECK_STATUS(ConvertOp(unary_expression->op(), &op))
            *output = node_manager->MakeUnaryExprNode(operand, op);
            break;
        }

        default: {
            status.msg = "Unsupport ASTExpression " + ast_expression->GetNodeKindString();
            status.code = common::kSqlError;
            return status;
        }
    }
    return status;
}

base::Status ConvertWindowDefinition(const zetasql::ASTWindowDefinition* zetasql_window,
                                     node::NodeManager* node_manager, status node::WindowDefNode** output) {
    auto* window_spec = zetasql_window->window_spec();
    node::ExprNodeList* partition_by = nullptr;
    node::OrderByNode* order_by = nullptr;
    CHECK_STATUS(
        ConvertExprNodeList(window_spec->partition_by()->partitioning_expressions(), node_manager, &partition_by))
    CHECK_STATUS(ConvertExprNode(window_spec->order_by(), node_manager, &order_by))
    auto* window_def = node_manager->MakeWindowDefNode(partition_by, );
    return window_def;
}

}  // namespace plan
}  // namespace hybridse
#endif  // HYBRIDSE_AST_NODE_CONVERTER_H
