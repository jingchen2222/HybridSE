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

#include "passes/resolve_udf_def.h"
#include "gtest/gtest.h"
#include "plan/plan_api.h"

namespace hybridse {
namespace passes {

class ResolveUdfDefTest : public ::testing::Test {};
//
// TEST_F(ResolveUdfDefTest, TestResolve) {
//    Status status;
//    node::NodeManager nm;
//    const std::string udf1 =
//        "%%fun\n"
//        "def test(x:i32, y:i32):i32\n"
//        "    return x+y\n"
//        "end\n";
//    node::PlanNodeList trees;
//    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(udf1, trees, &nm, status)) << status;
//    ASSERT_EQ(1u, trees.size());
//
//    auto def_plan = dynamic_cast<node::FuncDefPlanNode *>(trees[0]);
//    ASSERT_TRUE(def_plan != nullptr);
//
//    ResolveUdfDef resolver;
//    status = resolver.Visit(def_plan->fn_def_);
//    ASSERT_TRUE(status.isOK());
//}
//
// TEST_F(ResolveUdfDefTest, TestResolveFailed) {
//    Status status;
//    node::NodeManager nm;
//    const std::string udf1 =
//        "%%fun\n"
//        "def test(x:i32, y:i32):i32\n"
//        "    return x+z\n"
//        "end\n";
//    node::PlanNodeList trees;
//    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(udf1, trees, &nm, status)) << status;
//    ASSERT_EQ(1u, trees.size());
//
//    auto def_plan = dynamic_cast<node::FuncDefPlanNode *>(trees[0]);
//    ASSERT_TRUE(def_plan != nullptr);
//
//    ResolveUdfDef resolver;
//    status = resolver.Visit(def_plan->fn_def_);
//    ASSERT_TRUE(!status.isOK());
//}

}  // namespace passes
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
