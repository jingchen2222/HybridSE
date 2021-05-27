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

base::Status ConvertExprNodeList(const absl::Span<const ASTExpression* const>& expression_list,
                                 node::NodeManager* node_manager, node::ExprNodeList** output) {
    auto expr_list = node_manager->MakeExprList();
    for (auto expression : expression_list) {
        node::ExprNode* expr = nullptr;
        CHECK_STATUS(ConvertExpr(expression, node_manager, &expr))
        expr_list->AddChild(expr);
    }
    return expr_list;
}
base::Status ConvertOrderBy(const zetasql::ASTOrderBy* order_by, node::NodeManager* node_manager,
                            node::OrderByNode** output) {
    CHECK_STATUS(ConvertExprNodeList(order_by->ordering_expressions(), node_manager, &ordering_expressions))

    auto ordering_expressions = node_manager->MakeExprList();
    std::vector<bool> is_asc_list;
    for (auto ordering_expression : order_by->ordering_expressions()) {
        node::ExprNode* expr = nullptr;
        CHECK_STATUS(ConvertExpr(ordering_expression->expression(), node_manager, &expr))
        ordering_expressions->AddChild(expr);
        is_asc_list.push_back(!ordering_expression->descending());
    }

    *output = node_manager->MakeOrderByNode(ordering_expressions, is_asc_list);
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
            return base::Status::OK();
        }
        case zetasql::AST_PATH_EXPRESSION: {
            auto* path_expression = ast_expression->GetAsOrDie<zetasql::ASTPathExpression>();
            output = node_manager->MakeColumnRefNode(path_expression->last_name(), path_expression->first_name());
            return base::Status::OK();
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
            return base::Status::OK();
        }
        case zetasql::AST_UNARY_EXPRESSION: {
            auto* unary_expression = ast_expression->GetAsOrDie<zetasql::ASTUnaryExpression>();
            node::ExprNode* operand = nullptr;
            CHECK_STATUS(ConvertExprNode(unary_expression->operand(), node_manager, &operand))
            CHECK_STATUS(ConvertOp(unary_expression->op(), &op))
            *output = node_manager->MakeUnaryExprNode(operand, op);
            return base::Status::OK();
        }
        case zetasql::AST_INT_LITERAL: {
            const zetasql::ASTIntLiteral* literal = ast_expression->GetAsOrDie<zetasql::ASTIntLiteral>();
            int64_t int_value;
            hybridse::codec::StringRef str(literal->image().data());
            bool is_null;
            hybridse::udf::v1::string_to_bigint(&str, &int_value, &is_null);
            if (is_null) {
                status.msg = "Invalid floating point literal: " + std::string(literal->image());
                status.code = common::kSqlError;
                return status;
            }
            if (int_value <= INT_MAX && int_value >= INT_MIN) {
                *output = node_manager->MakeConstNode(static_cast<int>(int_value));
            } else {
                *output = node_manager->MakeConstNode(int_value);
            }
            return base::Status::OK();
        }

        case zetasql::AST_STRING_LITERAL: {
            const zetasql::ASTStringLiteral* literal = ast_expression->GetAsOrDie<zetasql::ASTStringLiteral>();
            *output = node_manager->MakeConstNode(literal->string_value());
            return base::Status::OK();
        }

        case zetasql::AST_BOOLEAN_LITERAL: {
            const zetasql::ASTBooleanLiteral* literal = ast_expression->GetAsOrDie<zetasql::ASTBooleanLiteral>();
            bool bool_value;
            hybridse::codec::StringRef str(literal->image().data());
            bool is_null;
            hybridse::udf::v1::string_to_bool(&str, &bool_value, &is_null);
            if (is_null) {
                status.msg = "Invalid bool literal: " + std::string(literal->image());
                status.code = common::kSqlError;
                return status;
            }
            *output = node_manager->MakeConstNode(bool_value);
            return base::Status::OK();
        }
        case zetasql::AST_FLOAT_LITERAL: {
            const zetasql::ASTFloatLiteral* literal = ast_expression->GetAsOrDie<zetasql::ASTFloatLiteral>();
            double double_value;
            hybridse::codec::StringRef str(literal->image().data());
            bool is_null;
            hybridse::udf::v1::string_to_double(&str, &double_value, &is_null);
            if (is_null) {
                status.msg = "Invalid floating point literal: " + std::string(literal->image());
                status.code = common::kSqlError;
                return status;
            }
            *output = node_manager->MakeConstNode(double_value);
            return base::Status::OK();
        }

        case zetasql::AST_NULL_LITERAL: {
            // NULL literals are always treated as int64_t.  Literal coercion rules
            // may make the NULL change type.
            *output = node_manager->MakeConstNode();
            return base::Status::OK();
        }

        case zetasql::AST_DATE_OR_TIME_LITERAL:
        case zetasql::AST_NUMERIC_LITERAL:
        case zetasql::AST_BIGNUMERIC_LITERAL:
        case zetasql::AST_JSON_LITERAL:
        case zetasql::AST_BYTES_LITERAL: {
            status.msg = "Un-support literal expression for node kind " + ast_expression->GetNodeKindString();
            status.code = common::kSqlError;
            return status;
        }

        default: {
            status.msg = "Unsupport ASTExpression " + ast_expression->GetNodeKindString();
            status.code = common::kSqlError;
            return status;
        }
    }
    return status;
}

base::Status ConvertFrameBound(const zetasql::ASTWindowFrameExpr* window_frame_expr, node::NodeManager* node_manager,
                               node::FrameBound** output) {
    if (nullptr == window_frame_expr) {
        *output = nullptr;
        return base::Status::OK();
    }
    base::Status status;
    node::ExprNode* expr;
    CHECK_STATUS(ConvertExprNode(window_frame_expr->expression(), node_manager, &expr));
    node::BoundType bound_type = node::BoundType::kCurrent;
    switch (window_frame_expr->boundary_type()) {
        case zetasql::ASTWindowFrameExpr::BoundaryType::CURRENT_ROW: {
            bound_type = node::BoundType::kCurrent;
            break;
        }
        case zetasql::ASTWindowFrameExpr::BoundaryType::OFFSET_PRECEDING: {
            bound_type = node::BoundType::kPreceding;
            break;
        }
        case zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_PRECEDING: {
            bound_type = node::BoundType::kPrecedingUnbound;
            break;
        }
        case zetasql::ASTWindowFrameExpr::BoundaryType::OFFSET_FOLLOWING: {
            bound_type = node::BoundType::kFollowing;
            break;
        }
        case zetasql::ASTWindowFrameExpr::BoundaryType::UNBOUNDED_FOLLOWING: {
            bound_type = node::BoundType::kFollowingUnbound;
            break;
        }
        default: {
            status.msg = "Un-support boundary type " + window_frame_expr->GetBoundaryTypeString();
            status.code = common::kSqlError;
            return status;
        }
    }
    *output = dynamic_cast<node::FrameBound*>(node_manager->MakeFrameBound(bound_type, expr));
    return base::Status::OK();
}
base::Status ConvertFrameNode(const zetasql::ASTWindowFrame* window_frame, node::NodeManager* node_manager,
                              node::FrameNode** output) {
    if (nullptr == window_frame) {
        *output = nullptr;
        return base::Status::OK();
    }
    base::Status status;
    node::FrameType frame_type;
    switch (window_frame->frame_unit()) {
        case zetasql::ASTWindowFrame::FrameUnit::ROWS: {
            frame_type = node::kFrameRows;
            break;
        }
        case zetasql::ASTWindowFrame::FrameUnit::RANGE: {
            frame_type = node::kFrameRows;
            break;
        }
        case zetasql::ASTWindowFrame::FrameUnit::ROWS_RANGE: {
            frame_type = node::kFrameRowsRange;
            break;
        }
        default: {
            status.msg = "Un-support frame type " + window_frame->GetFrameUnitString();
            status.code = common::kSqlError;
            return status;
        }
    }
    node::FrameBound* start = nullptr;
    node::FrameExtent* end = nullptr;
    CHECK_STATUS(ConvertFrameBound(window_frame->start_expr(), node_manager, &start))
    CHECK_STATUS(ConvertFrameBound(window_frame->end_expr(), node_manager, &end))
    node::ExprNode* frame_size = nullptr;
    CHECK_STATUS(ConvertExprNode(window_frame->max_size()->max_size(), node_manager, &frame_size))
    *output = node_manager->MakeFrameNode(frame_type, node_manager->MakeFrameExtent(start, end), frame_size);
    return base::Status::OK();
}
base::Status ConvertWindowDefinition(const zetasql::ASTWindowDefinition* zetasql_window,
                                     node::NodeManager* node_manager, node::WindowDefNode** output) {
    auto* window_spec = zetasql_window->window_spec();
    node::ExprNodeList* partition_by = nullptr;
    node::OrderByNode* order_by = nullptr;
    node::FrameNode* frame_node = nullptr;
    CHECK_STATUS(
        ConvertExprNodeList(window_spec->partition_by()->partitioning_expressions(), node_manager, &partition_by))
    CHECK_STATUS(ConvertExprNode(window_spec->order_by(), node_manager, &order_by))
    CHECK_STATUS(ConvertWindowFrame(window_spec->window_frame(), node_manager, &window_frame))
    // TODO: fill the following flags
    bool instance_is_not_in_window = false;
    bool exclude_current_time = false;
    *output = node_manager->MakeWindowDefNode(partition_by, order_by, window_frame, exclude_current_time,
                                              instance_is_not_in_window);
    return base::Status::OK();
}

}  // namespace plan
}  // namespace hybridse
#endif  // HYBRIDSE_AST_NODE_CONVERTER_H
