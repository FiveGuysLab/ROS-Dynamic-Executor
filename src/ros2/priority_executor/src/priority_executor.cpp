// Copyright 2015 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "priority_executor/priority_executor.hpp"
#include "priority_executor/priority_memory_strategy.hpp"
#include "rclcpp/any_executable.hpp"
#include "rcpputils/scope_exit.hpp"
#include "rclcpp/utilities.hpp"
#include <memory>
#include <sched.h>
#include <fstream>
#include <chrono>
// for sleep
#include <unistd.h>
namespace timed_executor
{

  TimedExecutor::TimedExecutor(const rclcpp::ExecutorOptions &options, std::string name)
      : rclcpp::Executor(options)
  {
    this->name = name;
    logger_ = create_logger();
    timing_results.reserve(MAX_TIMING_RESULTS + 1);
  }

  TimedExecutor::~TimedExecutor() {}

  void
  TimedExecutor::spin()
  {
    if (spinning.exchange(true))
    {
      throw std::runtime_error("spin() called while already spinning");
    }
    RCPPUTILS_SCOPE_EXIT(this->spinning.store(false));
    while (rclcpp::ok(this->context_) && spinning.load())
    {
      rclcpp::AnyExecutable any_executable;
      // std::cout<<memory_strategy_->number_of_ready_timers()<<std::endl;
      // std::cout << "spinning " << this->name << std::endl;
      // size_t ready = memory_strategy_->number_of_ready_subscriptions();
      // std::cout << "ready:" << ready << std::endl;

      if (get_next_executable(any_executable, std::chrono::nanoseconds(-1)))
      {
        execute_any_executable(any_executable);
        // make sure memory_strategy_ is an instance of PriorityMemoryStrategy
        if (prio_memory_strategy_!=nullptr)
        {
          prio_memory_strategy_->post_execute(any_executable);
        }
      }
    }
    RCLCPP_INFO(rclcpp::get_logger("priority_executor"), "priority executor shutdown");
  }

  bool TimedExecutor::get_next_executable(rclcpp::AnyExecutable &any_executable, std::chrono::nanoseconds timeout)
  {
    bool success = false;
    // Check to see if there are any subscriptions or timers needing service
    // TODO(wjwwood): improve run to run efficiency of this function
    // sched_yield();
    // sleep for 10us
    // usleep(20);
    auto start = std::chrono::steady_clock::now();
    wait_for_work(timeout);
    auto end = std::chrono::steady_clock::now();
    auto wait_duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::ostringstream oss;
    oss << "{\"operation\": \"wait_for_work\", \"wait_duration\": " << wait_duration.count() << "}";
    log_entry(logger_, oss.str());

    start = std::chrono::steady_clock::now();
    success = get_next_ready_executable(any_executable);
    end = std::chrono::steady_clock::now();
    auto get_next_duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    oss.str("");
    oss << "{\"operation\": \"get_next_executable\", \"duration\": " << get_next_duration.count() << ", \"result\": " << success << "}";
    log_entry(logger_, oss.str());
    return success;
  }

  // TODO: since we're calling this more often, clean it up a bit
  void
  TimedExecutor::wait_for_work(std::chrono::nanoseconds timeout)
  {
      {
    std::lock_guard<std::mutex> guard(mutex_);

    // Check weak_nodes_ to find any callback group that is not owned
    // by an executor and add it to the list of callbackgroups for
    // collect entities. Also exchange to false so it is not
    // allowed to add to another executor
    add_callback_groups_from_nodes_associated_to_executor();

    // Collect the subscriptions and timers to be waited on
    memory_strategy_->clear_handles();
    bool has_invalid_weak_groups_or_nodes =
      memory_strategy_->collect_entities(weak_groups_to_nodes_);

    if (has_invalid_weak_groups_or_nodes) {
      std::vector<rclcpp::CallbackGroup::WeakPtr> invalid_group_ptrs;
      for (auto pair : weak_groups_to_nodes_) {
        auto weak_group_ptr = pair.first;
        auto weak_node_ptr = pair.second;
        if (weak_group_ptr.expired() || weak_node_ptr.expired()) {
          invalid_group_ptrs.push_back(weak_group_ptr);
        }
      }
      std::for_each(
        invalid_group_ptrs.begin(), invalid_group_ptrs.end(),
        [this](rclcpp::CallbackGroup::WeakPtr group_ptr) {
          if (weak_groups_to_nodes_associated_with_executor_.find(group_ptr) !=
          weak_groups_to_nodes_associated_with_executor_.end())
          {
            weak_groups_to_nodes_associated_with_executor_.erase(group_ptr);
          }
          if (weak_groups_associated_with_executor_to_nodes_.find(group_ptr) !=
          weak_groups_associated_with_executor_to_nodes_.end())
          {
            weak_groups_associated_with_executor_to_nodes_.erase(group_ptr);
          }
          auto callback_guard_pair = weak_groups_to_guard_conditions_.find(group_ptr);
          if (callback_guard_pair != weak_groups_to_guard_conditions_.end()) {
            auto guard_condition = callback_guard_pair->second;
            weak_groups_to_guard_conditions_.erase(group_ptr);
            memory_strategy_->remove_guard_condition(guard_condition);
          }
          weak_groups_to_nodes_.erase(group_ptr);
        });
    }
      // clear wait set
      rcl_ret_t ret = rcl_wait_set_clear(&wait_set_);
      if (ret != RCL_RET_OK)
      {
        rclcpp::exceptions::throw_from_rcl_error(ret, "Couldn't clear wait set");
      }

      // The size of waitables are accounted for in size of the other entities
      ret = rcl_wait_set_resize(
          &wait_set_, memory_strategy_->number_of_ready_subscriptions(),
          memory_strategy_->number_of_guard_conditions(), memory_strategy_->number_of_ready_timers(),
          memory_strategy_->number_of_ready_clients(), memory_strategy_->number_of_ready_services(),
          memory_strategy_->number_of_ready_events());
      if (RCL_RET_OK != ret)
      {
        rclcpp::exceptions::throw_from_rcl_error(ret, "Couldn't resize the wait set");
      }

      if (!memory_strategy_->add_handles_to_wait_set(&wait_set_))
      {
        throw std::runtime_error("Couldn't fill wait set");
      }
    }
    
    // Timing measurement - start
    auto before_wait = std::chrono::steady_clock::now();
    
    rcl_ret_t status =
        rcl_wait(&wait_set_, std::chrono::duration_cast<std::chrono::nanoseconds>(timeout).count());
    
    // Timing measurement - end
    auto after_wait = std::chrono::steady_clock::now();
    auto wait_duration = after_wait - before_wait;
    const long duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(wait_duration).count();
    
    if (this->timing_results.size() < MAX_TIMING_RESULTS) {
      // Store the wait duration in the timing results
      this->timing_results.push_back(duration_ns);
    } else if (this->timing_results.size() == MAX_TIMING_RESULTS) {
      // Write to output file
      std::ofstream logFile("/home/guy/test_logs/executor_timing_results.txt");
      for (int i = 0; i < MAX_TIMING_RESULTS; i++) {
        logFile << this->timing_results.at(i) << std::endl;
      }
      logFile.close();
      exit(0);
      
      this->timing_results.push_back(-1);
    }
    
    if (status == RCL_RET_WAIT_SET_EMPTY)
    {
      RCUTILS_LOG_WARN_NAMED(
          "rclcpp",
          "empty wait set received in rcl_wait(). This should never happen.");
    }
    else if (status != RCL_RET_OK && status != RCL_RET_TIMEOUT)
    {
      using rclcpp::exceptions::throw_from_rcl_error;
      throw_from_rcl_error(status, "rcl_wait() failed");
    }

    // check the null handles in the wait set and remove them from the handles in memory strategy
    // for callback-based entities
    memory_strategy_->remove_null_handles(&wait_set_);
  }
  bool
  TimedExecutor::get_next_ready_executable(rclcpp::AnyExecutable &any_executable)
  {
    bool success = false;
    if (use_priorities)
    {
      std::shared_ptr<PriorityMemoryStrategy<>> strat = std::dynamic_pointer_cast<PriorityMemoryStrategy<>>(memory_strategy_);
      strat->get_next_executable(any_executable, weak_groups_to_nodes_);
      if (any_executable.timer || any_executable.subscription || any_executable.service || any_executable.client || any_executable.waitable)
      {
        success = true;
      }
    }
    else
    {
      // Check the timers to see if there are any that are ready
      memory_strategy_->get_next_timer(any_executable, weak_groups_to_nodes_);
      if (any_executable.timer)
      {
        std::cout << "got timer" << std::endl;
        success = true;
      }
      if (!success)
      {
        // Check the subscriptions to see if there are any that are ready
        memory_strategy_->get_next_subscription(any_executable, weak_groups_to_nodes_);
        if (any_executable.subscription)
        {
          // std::cout << "got subs" << std::endl;
          success = true;
        }
      }
      if (!success)
      {
        // Check the services to see if there are any that are ready
        memory_strategy_->get_next_service(any_executable, weak_groups_to_nodes_);
        if (any_executable.service)
        {
          std::cout << "got serv" << std::endl;
          success = true;
        }
      }
      if (!success)
      {
        // Check the clients to see if there are any that are ready
        memory_strategy_->get_next_client(any_executable, weak_groups_to_nodes_);
        if (any_executable.client)
        {
          std::cout << "got client" << std::endl;
          success = true;
        }
      }
      if (!success)
      {
        // Check the waitables to see if there are any that are ready
        memory_strategy_->get_next_waitable(any_executable, weak_groups_to_nodes_);
        if (any_executable.waitable)
        {
          std::cout << "got wait" << std::endl;
          success = true;
        }
      }
    }
    // At this point any_exec should be valid with either a valid subscription
    // or a valid timer, or it should be a null shared_ptr
    if (success)
    {
      // If it is valid, check to see if the group is mutually exclusive or
      // not, then mark it accordingly
      if (
          any_executable.callback_group &&
          any_executable.callback_group->type() == rclcpp::CallbackGroupType::MutuallyExclusive)
      {
        // It should not have been taken otherwise
        assert(any_executable.callback_group->can_be_taken_from().load());
        // Set to false to indicate something is being run from this group
        // This is reset to true either when the any_exec is executed or when the
        // any_exec is destructued
        any_executable.callback_group->can_be_taken_from().store(false);
      }
    }
    // If there is no ready executable, return false
    return success;
  }

  void TimedExecutor::set_use_priorities(bool use_prio)
  {
    use_priorities = use_prio;
  }

} // namespace timed_executor
