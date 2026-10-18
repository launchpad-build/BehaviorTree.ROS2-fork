#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include "behaviortree_cpp/utils/wakeup_signal.hpp"
#include "behaviortree_ros2/bt_action_node.hpp"
#include "btcpp_ros2_interfaces/action/sleep.hpp"

using namespace std::chrono_literals;
using Sleep = btcpp_ros2_interfaces::action::Sleep;
using ServerGoal = rclcpp_action::ServerGoalHandle<Sleep>;
using ClientEntity = rclcpp_action::ClientBase::EntityType;

namespace
{
enum GoalMode { IMMEDIATE, HOLD, DELAY_CANCEL, DELAY_ACCEPT, REJECT_CANCEL };

// Wait only at the ROS boundary; a stopped action never receives extra tree ticks.
template <class Predicate>
bool waitUntil(Predicate predicate)
{
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while(!predicate() && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(2ms);
  return predicate();
}

// A real ROS action boundary with controllable acceptance and completion.
class ActionServer
{
public:
  ActionServer(const rclcpp::Node::SharedPtr& node, const std::string& name)
  {
    server_ = rclcpp_action::create_server<Sleep>(
        node, name,
        [this](const auto&, const auto goal) {
          if(goal->msec_timeout == DELAY_ACCEPT)
          {
            response_pending = true;
            release_response.get_future().wait_for(2s);
          }
          return rclcpp_action::GoalResponse::ACCEPT_AND_EXECUTE;
        },
        [this](const auto& goal) {
          ++cancel_requests;
          return goal->get_goal()->msec_timeout == REJECT_CANCEL ?
                     rclcpp_action::CancelResponse::REJECT : rclcpp_action::CancelResponse::ACCEPT;
        },
        [this](const auto goal) {
          std::lock_guard<std::mutex> lock(mutex_);
          goals_.push_back(goal);
          const auto mode = goal->get_goal()->msec_timeout;
          if(mode == IMMEDIATE)
            finish(goal, false);
        });
    timer_ = node->create_wall_timer(2ms, [this] {
      std::lock_guard<std::mutex> lock(mutex_);
      for(const auto& goal : goals_)
      {
        if(goal->is_canceling() && goal->get_goal()->msec_timeout != DELAY_CANCEL)
        {
          finish(goal, true);
        }
        else if(goal->is_executing() && goal->get_goal()->msec_timeout == REJECT_CANCEL)
        {
          auto feedback = std::make_shared<Sleep::Feedback>();
          feedback->cycle = 1;
          goal->publish_feedback(feedback);
        }
      }
    });
  }

  bool waitForGoals(size_t count)
  {
    return waitUntil([&] {
      std::lock_guard<std::mutex> lock(mutex_);
      return goals_.size() >= count;
    });
  }

  bool waitForTerminal(size_t index)
  {
    return waitUntil([&] {
      std::lock_guard<std::mutex> lock(mutex_);
      return goals_.size() > index && !goals_[index]->is_active();
    });
  }

  void complete(size_t index, bool canceled = false)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    finish(goals_.at(index), canceled);
  }

  std::atomic<int> cancel_requests{0};
  std::atomic<bool> response_pending{false};
  std::promise<void> release_response;

private:
  static void finish(const std::shared_ptr<ServerGoal>& goal, bool canceled)
  {
    auto result = std::make_shared<Sleep::Result>();
    result->done = !canceled;
    if(canceled)
      goal->canceled(result);
    else
      goal->succeed(result);
  }

  std::mutex mutex_;
  std::vector<std::shared_ptr<ServerGoal>> goals_;
  rclcpp_action::Server<Sleep>::SharedPtr server_;
  rclcpp::TimerBase::SharedPtr timer_;
};

class RecordingAction : public BT::RosActionNode<Sleep>
{
public:
  using BT::TreeNode::setWakeUpInstance;

  RecordingAction(const BT::NodeConfig& config, const BT::RosNodeParams& params)
    : RosActionNode("action", config, params), tick_thread_(std::this_thread::get_id())
  {}

  bool setGoal(Goal& goal) override
  {
    goal.msec_timeout = mode;
    return true;
  }
  BT::NodeStatus onResultReceived(const WrappedResult& result) override
  {
    ++results;
    return result.result->done ? BT::NodeStatus::SUCCESS : BT::NodeStatus::FAILURE;
  }
  BT::NodeStatus onFailure(BT::ActionNodeErrorCode error) override
  {
    last_error = error;
    ++failures;
    return BT::NodeStatus::FAILURE;
  }
  BT::NodeStatus onFeedback(const std::shared_ptr<const Feedback>) override
  {
    if(std::this_thread::get_id() != tick_thread_)
      hooks_on_tick_thread = false;
    ++feedbacks;
    return BT::NodeStatus::SUCCESS;
  }
  std::shared_ptr<std::atomic<bool>> observeArrival(ClientEntity expected)
  {
    auto arrived = std::make_shared<std::atomic<bool>>(false);
    client_instance_->action_client->set_on_ready_callback(
        [arrived, expected](size_t, int entity) {
          if(entity == static_cast<int>(expected))
            *arrived = true;
        });
    return arrived;
  }
  // Tree controls reset status separately after invoking the node's halt hook.
  void haltAndReset()
  {
    haltNode();
    resetStatus();
  }

  int mode = IMMEDIATE;
  std::atomic<int> results{0}, failures{0}, feedbacks{0};
  std::atomic<BT::ActionNodeErrorCode> last_error{BT::INVALID_GOAL};
  std::atomic<bool> hooks_on_tick_thread{true};

private:
  std::thread::id tick_thread_;
};

class ActionLifecycle : public testing::Test
{
protected:
  static void SetUpTestSuite() { rclcpp::init(0, nullptr); }
  static void TearDownTestSuite() { rclcpp::shutdown(); }

  void SetUp() override
  {
    static std::atomic<int> sequence{0};
    const auto suffix = std::to_string(++sequence);
    server_node_ = std::make_shared<rclcpp::Node>("server_" + suffix);
    client_node_ = std::make_shared<rclcpp::Node>("client_" + suffix);
    server_ = std::make_unique<ActionServer>(server_node_, "sleep_" + suffix);
    executor_.add_node(server_node_);
    executor_.add_node(client_node_);
    executor_thread_ = std::thread([this] { executor_.spin(); });

    BT::NodeConfig config;
    config.blackboard = BT::Blackboard::create();
    BT::RosNodeParams params(client_node_, "sleep_" + suffix);
    params.server_timeout = 250ms;
    params.wait_for_server_timeout = 2s;
    action_ = std::make_unique<RecordingAction>(config, params);
    wake_up_ = std::make_shared<BT::WakeUpSignal>();
    action_->setWakeUpInstance(wake_up_);
  }

  void TearDown() override
  {
    executor_.cancel();
    if(executor_thread_.joinable())
      executor_thread_.join();
    action_.reset();
  }

  BT::NodeStatus tickToTerminal()
  {
    auto status = action_->executeTick();
    waitUntil([&] {
      if(status == BT::NodeStatus::RUNNING)
        status = action_->executeTick();
      return status != BT::NodeStatus::RUNNING;
    });
    return status;
  }

  rclcpp::Node::SharedPtr server_node_, client_node_;
  std::unique_ptr<ActionServer> server_;
  std::unique_ptr<RecordingAction> action_;
  std::shared_ptr<BT::WakeUpSignal> wake_up_;
  rclcpp::executors::SingleThreadedExecutor executor_;
  std::thread executor_thread_;
};

TEST_F(ActionLifecycle, CompletedGoalCanBeHaltedBeforeTheNextTreeTick)
{
  ASSERT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  ASSERT_TRUE(server_->waitForTerminal(0));
  ASSERT_TRUE(wake_up_->waitFor(2s));
  ASSERT_NO_THROW(action_->haltAndReset());
  EXPECT_EQ(server_->cancel_requests, 0);
}

TEST_F(ActionLifecycle, FeedbackAfterRejectedCancellationDoesNotWakeCompletedNode)
{
  action_->mode = REJECT_CANCEL;
  const auto arrived = action_->observeArrival(ClientEntity::FeedbackSubscription);
  ASSERT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  ASSERT_EQ(tickToTerminal(), BT::NodeStatus::SUCCESS);
  ASSERT_EQ(server_->cancel_requests, 1);
  ASSERT_EQ(action_->feedbacks, 1);
  EXPECT_TRUE(action_->hooks_on_tick_thread);

  wake_up_->waitFor(0ms);
  *arrived = false;
  EXPECT_FALSE(wake_up_->waitFor(250ms));
  EXPECT_TRUE(arrived->load());
  EXPECT_EQ(action_->feedbacks, 1);
  server_->complete(0);
}

TEST_F(ActionLifecycle, LateCanceledResultCannotCompleteTheNextGoal)
{
  action_->mode = DELAY_CANCEL;
  ASSERT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  ASSERT_TRUE(server_->waitForGoals(1));
  ASSERT_NO_THROW(action_->haltAndReset());
  ASSERT_EQ(server_->cancel_requests, 1);

  action_->mode = HOLD;
  ASSERT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  ASSERT_TRUE(server_->waitForGoals(2));
  const auto old_result_arrived = action_->observeArrival(ClientEntity::ResultClient);
  wake_up_->waitFor(0ms);
  server_->complete(0, true);
  ASSERT_TRUE(waitUntil([&] { return old_result_arrived->load(); }));
  EXPECT_FALSE(wake_up_->waitFor(250ms));
  EXPECT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  EXPECT_EQ(action_->results, 0);
  EXPECT_EQ(action_->failures, 0);
  server_->complete(1);
  EXPECT_EQ(tickToTerminal(), BT::NodeStatus::SUCCESS);
  EXPECT_EQ(action_->results, 1);
}

TEST_F(ActionLifecycle, AcceptanceAfterHaltStillCancelsTheAcceptedGoal)
{
  action_->mode = DELAY_ACCEPT;
  ASSERT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  ASSERT_TRUE(waitUntil([&] { return server_->response_pending.load(); }));
  ASSERT_NO_THROW(action_->haltAndReset());
  server_->release_response.set_value();
  // A stopped node receives no more ticks or test-supplied callback pumping.
  ASSERT_TRUE(server_->waitForTerminal(0));
  EXPECT_EQ(server_->cancel_requests, 1);
  EXPECT_EQ(action_->results, 0);
  EXPECT_EQ(action_->failures, 0);
}

TEST_F(ActionLifecycle, AcceptanceAfterGoalTimeoutStillCancelsTheAcceptedGoal)
{
  action_->mode = DELAY_ACCEPT;
  ASSERT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  ASSERT_TRUE(waitUntil([&] { return server_->response_pending.load(); }));
  ASSERT_EQ(tickToTerminal(), BT::NodeStatus::FAILURE);
  EXPECT_EQ(action_->last_error, BT::SEND_GOAL_TIMEOUT);
  server_->release_response.set_value();
  ASSERT_TRUE(server_->waitForTerminal(0));
  EXPECT_EQ(server_->cancel_requests, 1);
  EXPECT_EQ(action_->results, 0);
  EXPECT_EQ(action_->failures, 1);
}

}  // namespace
