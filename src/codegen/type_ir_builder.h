/*
 * type_ir_builder.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
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

#ifndef SRC_CODEGEN_TYPE_IR_BUILDER_H_
#define SRC_CODEGEN_TYPE_IR_BUILDER_H_

#include <node/node_enum.h>
#include <string>
#include <vector>
namespace fesql {
namespace codegen {

struct StringRef {
    int32_t size;
    char* data;
};

struct String {
    int32_t size;
    char* data;
};

inline const bool ConvertFeSQLType2DataType(
    const fesql::type::Type proto_type,
    node::DataType& data_type) {  // NOLINT
    switch (proto_type) {
        case fesql::type::kInt16:
            data_type = node::kTypeInt16;
            break;
        case fesql::type::kInt32:
            data_type = node::kTypeInt32;
            break;
        case fesql::type::kInt64:
            data_type = node::kTypeInt64;
            break;
        case fesql::type::kFloat:
            data_type = node::kTypeFloat;
            break;
        case fesql::type::kDouble:
            data_type = node::kTypeDouble;
            break;
        case fesql::type::kBool:
            data_type = node::kTypeBool;
            break;
        case fesql::type::kVarchar:
            data_type = node::kTypeString;
            break;
        default: {
            return false;
        }
    }
    return true;
}
inline const bool ConvertFeSQLType2LLVMType(const node::DataType& data_type,
                                            ::llvm::Module* m,  // NOLINT
                                            ::llvm::Type** llvm_type) {
    switch (data_type) {
        case node::kTypeVoid:
            *llvm_type = (::llvm::Type::getVoidTy(m->getContext()));
            break;
        case node::kTypeInt16:
            *llvm_type = (::llvm::Type::getInt16Ty(m->getContext()));
            break;
        case node::kTypeInt32:
            *llvm_type = (::llvm::Type::getInt32Ty(m->getContext()));
            break;
        case node::kTypeInt64:
            *llvm_type = (::llvm::Type::getInt64Ty(m->getContext()));
            break;
        case node::kTypeFloat:
            *llvm_type = (::llvm::Type::getFloatTy(m->getContext()));
            break;
        case node::kTypeDouble:
            *llvm_type = (::llvm::Type::getDoubleTy(m->getContext()));
            break;
        case node::kTypeInt8Ptr:
            *llvm_type = (::llvm::Type::getInt8PtrTy(m->getContext()));
            break;
        case node::kTypeString: {
            std::string name = "fe.string_ref";
            ::llvm::StringRef sr(name);
            ::llvm::StructType* stype = m->getTypeByName(sr);
            if (stype != NULL) {
                *llvm_type = stype;
                return true;
            }
            stype = ::llvm::StructType::create(m->getContext(), name);
            ::llvm::Type* size_ty = (::llvm::Type::getInt32Ty(m->getContext()));
            ::llvm::Type* data_ptr_ty =
                (::llvm::Type::getInt8PtrTy(m->getContext()));
            std::vector<::llvm::Type*> elements;
            elements.push_back(size_ty);
            elements.push_back(data_ptr_ty);
            stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
            *llvm_type = stype;
            return true;
        }
        default: {
            return false;
        }
    }
    return true;
}

struct Timestamp {
    int64_t ts;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_TYPE_IR_BUILDER_H_