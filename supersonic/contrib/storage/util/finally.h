// Copyright 2014 Google Inc.  All Rights Reserved
// Author: Wojtek Żółtak (wojciech.zoltak@gmail.com)
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


namespace supersonic {

// Utility which fires callback during destruction. Callback may anything
// compatible with std::function<void()>.
//
// Finally should be used when action is required when something throws. For
// example:
//
//   FailureOr<T> CreateT() {
//     std::unique_ptr<Sink> sink = CreateSink();
//
//     FailureOrOwned<X> x_result = CreateX();
//     PROPAGATE_ON_FAILURE(x_result);
//     std::unique_ptr<X> x(x_result.release());
//
//     return Success(new T(std::move(sink, std::move(x)));
//   }
//
// Sink requires a call to Finalize() during it's lifetime. In normal flow
// it is passed to T object and eventually that contract is satisfied. But
// when CreateX fails, the sink is deconstructed and contract is violated.
// It can be fixed using Finally:
//
//   FailureOr<T> CreateT() {
//     std::unique_ptr<Sink> sink = CreateSink();
//     Finally finally(Finalize(sink.get()));
//
//     FailureOrOwned<X> x_result = CreateX();
//     PROPAGATE_ON_FAILURE(x_result);
//     std::unique_ptr<X> x(x_result.release());
//
//     finally.abort();
//     return Success(new T(std::move(sink, std::move(x)));
//   }
//
// When CreateX fails the Finally instance is destroyed before Sink instance,
// which triggers callback with Finalize() in it. In normal flow, callback is
// aborted before return and has no effect.
class Finally {
 public:
  template <class Callable>
  Finally(const Callable& callable)
      : active_(true),
        callback_(callable) {}

  // Triggers execution of underlying callback.
  ~Finally() {
    if (active_){
      callback_();
    }
  }

  // Aborts callback execution.
  void Abort() {
    active_ = false;
  }

 private:
  bool active_;
  std::function<void()> callback_;
};


// Utility for managing multiple Finally instances. Typical usage looks like
// that:
//
//   FailureOr<T> CreateT() {
//     FinallyManager finally_manager;
//
//     std::unique_ptr<Sink> sink = CreateSink();
//     FINALLY(Finalize(sink.get())).ManagedBy(&finally_manager);
//
//     std::unique_ptr<Sink> sink2 = CreateSink();
//     FINALLY(Finalize(sink2.get())).ManagedBy(&finally_manager);
//
//     FailureOrOwned<X> x_result = CreateX();
//     PROPAGATE_ON_FAILURE(x_result);
//     std::unique_ptr<X> x(x_result.release());
//
//     finally_manager.abort();
//     return Success(new T(std::move(sink, std::move(x)));
//   }
class FinallyManager {
 public:
  // Registers Finally instance in manager.
  void Register(Finally* finally) {
    finallys_.push_back(finally);
  }

  // Calls Abort() on all registered Finally instances.
  void Abort() {
    for (Finally* finally : finallys_) {
      finally->Abort();
    }
  }

 private:
  std::vector<Finally*> finallys_;
};

// Macro for dealing with managed finally instances. See comment for
// FinallyManager for usage example.
#define FINALLY(action, manager) Finally finally__LINE__(action); \
    manager.Register(&finally__LINE__)


// Functor which finalizes objects with `FailureOrVoid Finalize()` method.
// Does not distinct success and failure of finalization.
template <class T>
class FinalizeFunctor {
 public:
  FinalizeFunctor(T* object) : object_(object) {}
  void operator()() {
    object_->Finalize().mark_checked();
  }

 private:
  T* object_;
};

// Helper function for creation of FinalizeFunctor objects.
template <class T>
const FinalizeFunctor<T> Finalize(T* object) {
  return FinalizeFunctor<T>(object);
}

}  // namespace supersonic
