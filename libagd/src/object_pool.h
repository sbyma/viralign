#pragma once

#include <memory>
#include <stack>
#include <iostream>
#include "absl/synchronization/mutex.h"

// adapted from https://stackoverflow.com/a/27837534
// should be threadsafe

template <class T>
class ObjectPool {
 private:
  struct External_Deleter {
    explicit External_Deleter(std::weak_ptr<ObjectPool<T>*> pool)
        : pool_(pool) {}
    External_Deleter() = default;

    void operator()(T* ptr) {
      if (auto pool_ptr = pool_.lock()) {
        if (!pool_ptr.get()) return;
        try {
          // return the obj to the pool
          (*pool_ptr.get())->add(std::unique_ptr<T>{ptr});
          return;
        } catch (...) {
        }
      }
      // pool is kill, delete the object
      std::default_delete<T>{}(ptr);
    }

   private:
    std::weak_ptr<ObjectPool<T>*> pool_;
  };

  void add(std::unique_ptr<T> t) {
    absl::MutexLock l(&mu_);
    pool_.push(std::move(t));
  }

 public:
  using ptr_type = std::unique_ptr<T, External_Deleter>;

  ObjectPool() : this_ptr_(new ObjectPool<T>*(this)) {}
  virtual ~ObjectPool() {
  }

  ptr_type get() {
    absl::MutexLock l(&mu_);

    if (!pool_.empty()) {
      // return the first available item
      ptr_type tmp(pool_.top().release(),
                   External_Deleter{std::weak_ptr<ObjectPool<T>*>{this_ptr_}});
      pool_.pop();
      return std::move(tmp);
    } else {
      // the pool is empty, create new and return that
      auto tmp = ptr_type(
          new T, External_Deleter{std::weak_ptr<ObjectPool<T>*>{this_ptr_}});
      return std::move(tmp);
    }
  }

  bool empty() const { return pool_.empty(); }

  size_t size() const { return pool_.size(); }

 private:
  std::shared_ptr<ObjectPool<T>*> this_ptr_;
  std::stack<std::unique_ptr<T> > pool_;
  absl::Mutex mu_;
};