#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
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
enum GoalMode { IMMEDIATE, HOLD, ABORT, REJECT, NEGATIVE_RESULT, DELAY_CANCEL, DELAY_ACCEPT,
                REJECT_CANCEL, FEEDBACK };

// A real ROS action boundary. Its timer is the only owner of held-goal completion.
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
            std::unique_lock<std::mutex> lock(response_mutex_);
            response_pending_ = true;
            response_changed_.notify_all();
            response_changed_.wait_for(lock, 2s, [&] { return release_response_; });
          }
          return goal->msec_timeout == REJECT ? rclcpp_action::GoalResponse::REJECT :
                                               rclcpp_action::GoalResponse::ACCEPT_AND_EXECUTE;
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
          if(mode == IMMEDIATE || mode == NEGATIVE_RESULT || mode == ABORT)
          {
            finish(goal, mode == ABORT ? rclcpp_action::ResultCode::ABORTED :
                                        rclcpp_action::ResultCode::SUCCEEDED,
                   mode != NEGATIVE_RESULT);
          }
          changed_.notify_all();
        });
    timer_ = node->create_wall_timer(2ms, [this] {
      std::lock_guard<std::mutex> lock(mutex_);
      for(const auto& goal : goals_)
      {
        if(goal->is_canceling() && goal->get_goal()->msec_timeout != DELAY_CANCEL)
        {
          finish(goal, rclcpp_action::ResultCode::CANCELED, false);
        }
        else if(goal->is_executing() &&
                (goal->get_goal()->msec_timeout == FEEDBACK ||
                 goal->get_goal()->msec_timeout == REJECT_CANCEL))
        {
          auto feedback = std::make_shared<Sleep::Feedback>();
          feedback->cycle = 1;
          goal->publish_feedback(feedback);
        }
      }
      for(const auto& command : commands_)
      {
        finish(goals_.at(command.index), command.result, true);
      }
      commands_.clear();
      changed_.notify_all();
    });
  }

  bool waitForGoals(size_t count)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, 2s, [&] { return goals_.size() >= count; });
  }

  bool waitForTerminal(size_t index)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, 2s, [&] {
      return goals_.size() > index && !goals_[index]->is_active();
    });
  }

  void complete(size_t index, rclcpp_action::ResultCode result)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    commands_.push_back({index, result});
  }

  bool waitForPendingResponse()
  {
    std::unique_lock<std::mutex> lock(response_mutex_);
    return response_changed_.wait_for(lock, 2s, [&] { return response_pending_; });
  }

  void releaseResponse()
  {
    std::lock_guard<std::mutex> lock(response_mutex_);
    release_response_ = true;
    response_changed_.notify_all();
  }

  std::atomic<int> cancel_requests{0};

private:
  static void finish(const std::shared_ptr<ServerGoal>& goal,
                     rclcpp_action::ResultCode code, bool done)
  {
    auto result = std::make_shared<Sleep::Result>();
    result->done = done;
    if(code == rclcpp_action::ResultCode::CANCELED)
      goal->canceled(result);
    else if(code == rclcpp_action::ResultCode::ABORTED)
      goal->abort(result);
    else
      goal->succeed(result);
  }

  struct Command
  {
    size_t index;
    rclcpp_action::ResultCode result;
  };
  std::mutex mutex_;
  std::condition_variable changed_;
  std::mutex response_mutex_;
  std::condition_variable response_changed_;
  bool response_pending_ = false, release_response_ = false;
  std::vector<std::shared_ptr<ServerGoal>> goals_;
  std::deque<Command> commands_;
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
    checkThread();
    ++results;
    return result.result->done ? BT::NodeStatus::SUCCESS : BT::NodeStatus::FAILURE;
  }
  BT::NodeStatus onFailure(BT::ActionNodeErrorCode error) override
  {
    checkThread();
    last_error = error;
    ++failures;
    return BT::NodeStatus::FAILURE;
  }
  BT::NodeStatus onFeedback(const std::shared_ptr<const Feedback>) override
  {
    checkThread();
    ++feedbacks;
    return feedback_status;
  }
  void onHalt() override { ++halts; }

  bool usesInternalExecutor() const { return client_instance_->use_internal_executor; }
  // Only the stale-result test deliberately stops the worker to control dispatch order.
  void drainReadyCallbacks()
  {
    if(client_instance_->use_internal_executor)
    {
      client_instance_->stop_executor = true;
      if(client_instance_->executor_thread.joinable())
        client_instance_->executor_thread.join();
      client_instance_->callback_executor.spin_all(100ms);
    }
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
  BT::NodeStatus feedback_status = BT::NodeStatus::RUNNING;
  std::atomic<int> results{0}, failures{0}, feedbacks{0}, halts{0};
  std::atomic<BT::ActionNodeErrorCode> last_error{BT::INVALID_GOAL};
  std::atomic<bool> hooks_on_tick_thread{true};

private:
  void checkThread()
  {
    if(std::this_thread::get_id() != tick_thread_)
      hooks_on_tick_thread = false;
  }
  std::thread::id tick_thread_;
};

class ActionLifecycle : public testing::TestWithParam<bool>
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
    if(GetParam())
      executor_.add_node(client_node_);
    executor_thread_ = std::thread([this] { executor_.spin(); });

    BT::NodeConfig config;
    config.blackboard = BT::Blackboard::create();
    BT::RosNodeParams params(client_node_, "sleep_" + suffix);
    params.use_external_executor = GetParam();
    params.server_timeout = 250ms;
    params.wait_for_server_timeout = 2s;
    action_ = std::make_unique<RecordingAction>(config, params);
    ASSERT_EQ(action_->usesInternalExecutor(), !GetParam());
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

  template <class Predicate>
  bool waitUntil(Predicate predicate)
  {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while(!predicate() && std::chrono::steady_clock::now() < deadline)
    {
      wake_up_->waitFor(2ms);
    }
    return predicate();
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

  // A callback wakes the BT; do not run another tree tick while it is being delivered.
  bool waitForCallback()
  {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while(std::chrono::steady_clock::now() < deadline)
    {
      if(wake_up_->waitFor(2ms))
        return true;
    }
    return false;
  }

  void drainCallbacks()
  {
    if(GetParam())
    {
      executor_.cancel();
      if(executor_thread_.joinable())
        executor_thread_.join();
      executor_.spin_all(100ms);
    }
    else
      action_->drainReadyCallbacks();
  }

  rclcpp::Node::SharedPtr server_node_, client_node_;
  std::unique_ptr<ActionServer> server_;
  std::unique_ptr<RecordingAction> action_;
  std::shared_ptr<BT::WakeUpSignal> wake_up_;
  rclcpp::executors::SingleThreadedExecutor executor_;
  std::thread executor_thread_;
};

TEST_P(ActionLifecycle, CompletedGoalCanBeHaltedBeforeTheNextTreeTick)
{
  action_->mode = IMMEDIATE;
  ASSERT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  ASSERT_TRUE(server_->waitForGoals(1));
  ASSERT_TRUE(server_->waitForTerminal(0));
  ASSERT_TRUE(waitForCallback());
  ASSERT_NO_THROW(action_->haltAndReset());
  EXPECT_EQ(action_->halts, 1);
  EXPECT_EQ(server_->cancel_requests, 0);

  action_->mode = IMMEDIATE;
  EXPECT_EQ(tickToTerminal(), BT::NodeStatus::SUCCESS);
  EXPECT_EQ(action_->results, 1);
}

TEST_P(ActionLifecycle, AbortRejectAndNegativeResultRemainFailures)
{
  for(auto mode : {ABORT, REJECT, NEGATIVE_RESULT})
  {
    action_->mode = mode;
    EXPECT_EQ(tickToTerminal(), BT::NodeStatus::FAILURE);
    if(mode == ABORT)
      EXPECT_EQ(action_->last_error, BT::ACTION_ABORTED);
    if(mode == REJECT)
      EXPECT_EQ(action_->last_error, BT::GOAL_REJECTED_BY_SERVER);
    action_->haltAndReset();
  }
  EXPECT_EQ(action_->results, 1);
  EXPECT_EQ(action_->failures, 2);
}

TEST_P(ActionLifecycle, FeedbackStopRunsOnTreeThreadAndCancelsTheGoal)
{
  action_->mode = FEEDBACK;
  action_->feedback_status = BT::NodeStatus::SUCCESS;
  ASSERT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  ASSERT_TRUE(server_->waitForGoals(1));
  ASSERT_EQ(tickToTerminal(), BT::NodeStatus::SUCCESS);
  EXPECT_EQ(action_->feedbacks, 1);
  EXPECT_TRUE(action_->hooks_on_tick_thread);
  EXPECT_EQ(server_->cancel_requests, 1);
  EXPECT_TRUE(server_->waitForTerminal(0));
  EXPECT_EQ(action_->results, 0);
}

TEST_P(ActionLifecycle, FeedbackAfterRejectedCancellationDoesNotWakeCompletedNode)
{
  action_->mode = REJECT_CANCEL;
  action_->feedback_status = BT::NodeStatus::SUCCESS;
  const auto arrived = action_->observeArrival(ClientEntity::FeedbackSubscription);
  ASSERT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  ASSERT_TRUE(server_->waitForGoals(1));
  ASSERT_EQ(tickToTerminal(), BT::NodeStatus::SUCCESS);
  ASSERT_EQ(server_->cancel_requests, 1);
  ASSERT_EQ(action_->feedbacks, 1);

  wake_up_->waitFor(0ms);
  *arrived = false;
  EXPECT_FALSE(wake_up_->waitFor(250ms));
  EXPECT_TRUE(arrived->load());
  EXPECT_EQ(action_->feedbacks, 1);
  server_->complete(0, rclcpp_action::ResultCode::SUCCEEDED);
  EXPECT_TRUE(server_->waitForTerminal(0));
}

TEST_P(ActionLifecycle, HaltCancelsActiveGoalAndTheNodeCanBeReused)
{
  action_->mode = HOLD;
  ASSERT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  ASSERT_TRUE(server_->waitForGoals(1));
  ASSERT_NO_THROW(action_->haltAndReset());
  EXPECT_EQ(server_->cancel_requests, 1);
  EXPECT_TRUE(server_->waitForTerminal(0));
  action_->mode = IMMEDIATE;
  EXPECT_EQ(tickToTerminal(), BT::NodeStatus::SUCCESS);
  EXPECT_EQ(action_->results, 1);
  EXPECT_EQ(action_->failures, 0);
}

TEST_P(ActionLifecycle, LateCanceledResultCannotCompleteTheNextGoal)
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
  server_->complete(0, rclcpp_action::ResultCode::CANCELED);
  ASSERT_TRUE(server_->waitForTerminal(0));
  ASSERT_TRUE(waitUntil([&] { return old_result_arrived->load(); }));
  // Receipt alone does not prove dispatch. Join any in-flight callback, then
  // drain the received result before the next tree tick. No production callback
  // state is exposed.
  drainCallbacks();
  EXPECT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  EXPECT_EQ(action_->results, 0);
  EXPECT_EQ(action_->failures, 0);
  const auto next_result_arrived = action_->observeArrival(ClientEntity::ResultClient);
  server_->complete(1, rclcpp_action::ResultCode::SUCCEEDED);
  ASSERT_TRUE(waitUntil([&] {
    drainCallbacks();
    return next_result_arrived->load();
  }));
  ASSERT_TRUE(server_->waitForTerminal(1));
  drainCallbacks();
  EXPECT_EQ(action_->executeTick(), BT::NodeStatus::SUCCESS);
  EXPECT_EQ(action_->results, 1);
}

TEST_P(ActionLifecycle, AcceptanceAfterHaltStillCancelsTheAcceptedGoal)
{
  action_->mode = DELAY_ACCEPT;
  ASSERT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  ASSERT_TRUE(server_->waitForPendingResponse());
  ASSERT_NO_THROW(action_->haltAndReset());
  server_->releaseResponse();
  ASSERT_TRUE(server_->waitForGoals(1));
  // A stopped node receives no more ticks or test-supplied callback pumping.
  ASSERT_TRUE(server_->waitForTerminal(0));
  EXPECT_EQ(server_->cancel_requests, 1);
  EXPECT_EQ(action_->results, 0);
  EXPECT_EQ(action_->failures, 0);

  action_->mode = IMMEDIATE;
  EXPECT_EQ(tickToTerminal(), BT::NodeStatus::SUCCESS);
  EXPECT_EQ(action_->results, 1);
}

TEST_P(ActionLifecycle, AcceptanceAfterGoalTimeoutStillCancelsTheAcceptedGoal)
{
  action_->mode = DELAY_ACCEPT;
  ASSERT_EQ(action_->executeTick(), BT::NodeStatus::RUNNING);
  ASSERT_TRUE(server_->waitForPendingResponse());
  ASSERT_EQ(tickToTerminal(), BT::NodeStatus::FAILURE);
  EXPECT_EQ(action_->last_error, BT::SEND_GOAL_TIMEOUT);
  server_->releaseResponse();
  ASSERT_TRUE(server_->waitForGoals(1));
  ASSERT_TRUE(server_->waitForTerminal(0));
  EXPECT_EQ(server_->cancel_requests, 1);
  EXPECT_EQ(action_->results, 0);
  EXPECT_EQ(action_->failures, 1);

  action_->haltAndReset();
  action_->mode = IMMEDIATE;
  EXPECT_EQ(tickToTerminal(), BT::NodeStatus::SUCCESS);
  EXPECT_EQ(action_->results, 1);
}

INSTANTIATE_TEST_SUITE_P(Executors, ActionLifecycle, testing::Bool(),
                        [](const auto& info) { return info.param ? "External" : "Internal"; });
}  // namespace
