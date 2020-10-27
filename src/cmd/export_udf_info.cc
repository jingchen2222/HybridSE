/*
 * gen_udf_doc.cc
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
#include <fstream>
#include <iostream>
#include <string>
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "yaml-cpp/yaml.h"

#include "passes/resolve_fn_and_attrs.h"
#include "udf/default_udf_library.h"
#include "udf/udf_registry.h"

DEFINE_string(output_dir, ".", "Output directory path");
DEFINE_string(output_file, "udf_defs.yaml", "Output yaml filename");

namespace fesql {
namespace cmd {

struct UDFTypeInfo {
    std::vector<const node::TypeNode*> arg_types;
    const node::TypeNode* return_type;
    UDFTypeInfo(const std::vector<const node::TypeNode*>& arg_types,
                const node::TypeNode* return_type)
        : arg_types(arg_types), return_type(return_type) {}
};

class UDFTypeExtractor {
 public:
    UDFTypeExtractor() {
        enum_types_.push_back(node_manager_.MakeTypeNode(node::kBool));
        enum_types_.push_back(node_manager_.MakeTypeNode(node::kDate));
        enum_types_.push_back(node_manager_.MakeTypeNode(node::kTimestamp));
        enum_types_.push_back(node_manager_.MakeTypeNode(node::kVarchar));
        enum_types_.push_back(node_manager_.MakeTypeNode(node::kInt16));
        enum_types_.push_back(node_manager_.MakeTypeNode(node::kInt32));
        enum_types_.push_back(node_manager_.MakeTypeNode(node::kInt64));
        enum_types_.push_back(node_manager_.MakeTypeNode(node::kFloat));
        enum_types_.push_back(node_manager_.MakeTypeNode(node::kDouble));
    }

    void Expand(const std::string& name, size_t idx, bool is_expansion,
                std::vector<const node::TypeNode*>* arg_types,
                std::vector<UDFTypeInfo>* output) {
        if (idx == arg_types->size()) {
            // rec end
            auto library = udf::DefaultUDFLibrary::get();
            node::NodeManager* nm = &node_manager_;
            std::vector<node::ExprNode*> args;
            for (size_t i = 0; i < arg_types->size(); ++i) {
                auto arg_node = nm->MakeExprIdNode("arg_" + std::to_string(i));
                arg_node->SetOutputType((*arg_types)[i]);
                arg_node->SetNullable(false);
                args.push_back(arg_node);
            }
            node::ExprNode* call = nullptr;
            auto status = library->Transform(name, args, nm, &call);
            if (!status.isOK()) {
                if (!is_expansion) {
                    LOG(WARNING) << "Invalid registry of " << name << ": <"
                                 << udf::GetArgSignature(args) << ">";
                }
                return;
            }
            node::ExprNode* resolved = nullptr;
            vm::SchemaSourceList schemas_source_list;
            vm::SchemasContext schemas_context(schemas_source_list);
            passes::ResolveFnAndAttrs resolver(nm, library, schemas_context);
            status = resolver.VisitExpr(call, &resolved);
            if (!status.isOK() || resolved == nullptr ||
                resolved->GetOutputType() == nullptr) {
                LOG(WARNING) << "Fail to resolve registry of " << name << ": <"
                             << udf::GetArgSignature(args) << ">";
                return;
            }
            UDFTypeInfo info(*arg_types, resolved->GetOutputType());
            output->push_back(info);
            return;
        }
        auto cur_type = (*arg_types)[idx];
        if (cur_type != nullptr) {
            Expand(name, idx + 1, is_expansion, arg_types, output);
        } else {
            for (auto expand_type : enum_types_) {
                (*arg_types)[idx] = expand_type;
                Expand(name, idx + 1, true, arg_types, output);
                (*arg_types)[idx] = nullptr;
            }
        }
    }

 private:
    node::NodeManager node_manager_;

    std::vector<const node::TypeNode*> enum_types_;
};

int ExportUDFInfo(const std::string& dir, const std::string& filename) {
    auto library = udf::DefaultUDFLibrary::get();
    auto& registries = library->GetAllRegistries();
    YAML::Emitter yaml_out;

    UDFTypeExtractor udf_extractor;

    yaml_out << YAML::BeginMap;
    for (auto& pair : registries) {
        std::string name = pair.first;
        auto signature_table = pair.second->signature_table.GetTable();

        yaml_out << YAML::Key << name;
        yaml_out << YAML::Value;
        yaml_out << YAML::BeginSeq;

        for (auto& pair : signature_table) {
            auto key = pair.first;
            auto& regitem = pair.second;
            auto registry = regitem.value;

            std::vector<UDFTypeInfo> expand_type_infos;
            udf_extractor.Expand(name, 0, false, &regitem.arg_types,
                                 &expand_type_infos);

            yaml_out << YAML::BeginMap;
            yaml_out << YAML::Key << "signatures";
            yaml_out << YAML::Value;
            yaml_out << YAML::BeginSeq;
            for (auto& type_info : expand_type_infos) {
                yaml_out << YAML::BeginMap;
                yaml_out << YAML::Key << "arg_types";
                yaml_out << YAML::Value;
                yaml_out << YAML::BeginSeq;
                for (auto ty : type_info.arg_types) {
                    yaml_out << ty->GetName();
                }
                yaml_out << YAML::EndSeq;
                yaml_out << YAML::Key << "return_type";
                yaml_out << YAML::Value << type_info.return_type->GetName();
                yaml_out << YAML::EndMap;
            }
            yaml_out << YAML::EndSeq;

            yaml_out << YAML::Key << "doc";
            yaml_out << YAML::Value << registry->doc();
            yaml_out << YAML::Key << "is_variadic";
            yaml_out << YAML::Value << regitem.is_variadic;
            yaml_out << YAML::EndMap;
        }

        yaml_out << YAML::EndSeq;
    }
    yaml_out << YAML::EndMap;

    // write c style header for udfs
    LOG(INFO) << "Export udf info to " << dir << "/" << filename;
    std::fstream header_file(dir + "/" + filename, std::fstream::out);
    if (header_file.fail()) {
        LOG(WARNING) << "Create doxygen header file failed";
        return -1;
    }
    header_file << yaml_out.c_str() << "\n";
    header_file.close();
    return 0;
}

}  // namespace cmd
}  // namespace fesql

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    return fesql::cmd::ExportUDFInfo(FLAGS_output_dir, FLAGS_output_file);
    return 0;
}