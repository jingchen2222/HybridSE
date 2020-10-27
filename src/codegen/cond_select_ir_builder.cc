/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * cond_select_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/8/18
 *--------------------------------------------------------------------------
 **/
#include "codegen/cond_select_ir_builder.h"
#include <vector>
#include "codegen/ir_base_builder.h"
namespace fesql {
namespace codegen {
CondSelectIRBuilder::CondSelectIRBuilder() {}
CondSelectIRBuilder::~CondSelectIRBuilder() {}

/**
 * Select left value if cond is true, otherwise select right value
 * @param block
 * @param cond
 * @param left
 * @param right
 * @param output
 * @return
 */
base::Status CondSelectIRBuilder::Select(::llvm::BasicBlock* block,
                                         const NativeValue& cond_value,
                                         const NativeValue& left,
                                         const NativeValue& right,
                                         NativeValue* output) {
    NativeValue left_value = left;
    NativeValue right_value = right;
    if (left.IsConstNull()) {
        left_value.SetType(right.GetType());
    }
    if (right.IsConstNull()) {
        right_value.SetType(left.GetType());
    }
    // build condition
    ::llvm::IRBuilder<> builder(block);
    base::Status status;
    auto raw_cond = cond_value.GetValue(&builder);
    if (cond_value.IsNullable()) {
        raw_cond = builder.CreateAnd(
            raw_cond, builder.CreateNot(cond_value.GetIsNull(&builder)));
    }

    if (left_value.IsTuple()) {
        CHECK_TRUE(right_value.IsTuple() &&
                       left_value.GetFieldNum() == right_value.GetFieldNum(),
                   kCodegenError);
        std::vector<NativeValue> result_tuple;
        for (size_t i = 0; i < left_value.GetFieldNum(); ++i) {
            NativeValue sub_left = left_value.GetField(i);
            NativeValue sub_right = right_value.GetField(i);
            ::llvm::Value* raw_value =
                builder.CreateSelect(raw_cond, sub_left.GetValue(&builder),
                                     sub_right.GetValue(&builder));

            bool output_nullable =
                sub_left.IsNullable() || sub_right.IsNullable();
            if (output_nullable) {
                ::llvm::Value* output_is_null =
                    builder.CreateSelect(raw_cond, sub_left.GetIsNull(&builder),
                                         sub_right.GetIsNull(&builder));
                result_tuple.push_back(*output = NativeValue::CreateWithFlag(
                                           raw_value, output_is_null));
            } else {
                result_tuple.push_back(NativeValue::Create(raw_value));
            }
        }
        *output = NativeValue::CreateTuple(result_tuple);
    } else {
        ::llvm::Value* raw_value =
            builder.CreateSelect(raw_cond, left_value.GetValue(&builder),
                                 right_value.GetValue(&builder));
        bool output_nullable =
            left_value.IsNullable() || right_value.IsNullable();
        if (output_nullable) {
            ::llvm::Value* output_is_null =
                builder.CreateSelect(raw_cond, left_value.GetIsNull(&builder),
                                     right_value.GetIsNull(&builder));
            *output = NativeValue::CreateWithFlag(raw_value, output_is_null);
        } else {
            *output = NativeValue::Create(raw_value);
        }
    }
    return base::Status::OK();
}
}  // namespace codegen
}  // namespace fesql