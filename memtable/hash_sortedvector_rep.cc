
#include <algorithm>
#include <atomic>
#include <vector>

#include "db/memtable.h"
#include "port/port.h"
#include "rocksdb/utilities/options_type.h"
#include "stl_wrappers.h"
#include "util/heap.h"
#include "util/murmurhash.h"

namespace ROCKSDB_NAMESPACE {
namespace {

struct MyKeyHandle {
  MyKeyHandle* GetNextBucketItem() {
    return next_.load(std::memory_order_acquire);
  }
  void SetNextBucketItem(MyKeyHandle* handle) {
    next_.store(handle, std::memory_order_release);
  }
  std::atomic<MyKeyHandle*> next_ = nullptr;
  char key_[1];
};

struct BucketHeader {
  port::RWMutex rw_lock_;
  std::atomic<MyKeyHandle*> items_ = nullptr;
  std::atomic<uint32_t> elements_num_ = 0;

  BucketHeader() {}
  bool Contains(const char* key, const MemTableRep::KeyComparator& comparator,
                bool need_lock) {
    bool index_exist = false;
    if (elements_num_.load() == 0) {
        return false;
    }
    if (need_lock) {
        rw_lock_.ReadLock();
    }
    MyKeyHandle* anchor = items_.load(std::memory_order_acquire);
    for (auto k = anchor; k != nullptr; k = k->GetNextBucketItem()) {
        const int cmp_ret = comparator(k->key_, key);
        if (cmp_ret == 0) {
          index_exist = true;
          break;
        }
        if (cmp_ret > 0) {
          break;
        }
    }
    if (need_lock) {
        rw_lock_.ReadUnlock();
    }
    return index_exist;
  }
  bool Add(MyKeyHandle* handle, const MemTableRep::KeyComparator& comparator) {
    WriteLock wl(&rw_lock_);
    MyKeyHandle* iter = items_.load(std::memory_order_acquire);
    MyKeyHandle* prev = nullptr;
    for (size_t i = 0; i < elements_num_; i++) {
        const int cmp_ret = comparator(iter->key_, handle->key_);
        if (cmp_ret == 0) {
          return false;
        }
        if (cmp_ret > 0) {
          break;
        }
        prev = iter;
        iter = iter->GetNextBucketItem();
    }
    handle->SetNextBucketItem(iter);
    if (prev) {
        prev->SetNextBucketItem(handle);
    } else {
        items_ = handle;
    }
    elements_num_++;
    return true;
  }
  void Get(const LookupKey& key, const MemTableRep::KeyComparator& comparator,
           void* callback_args, bool (*callback_func)(void* arg, const char* entry),
           bool need_lock) {
    if (elements_num_.load() == 0) {
        return;
    }
    if (need_lock) {
        rw_lock_.ReadLock();
    }
    auto iter = items_.load(std::memory_order_acquire);
    for (; iter != nullptr; iter = iter->GetNextBucketItem()) {
      if (comparator(iter->key_, key.internal_key()) >= 0) {
          break;
      }
      for (; iter != nullptr; iter = iter->GetNextBucketItem()) {
          if (!callback_func(callback_args, iter->key_)) {
            break;
          }
      }
    }
    if (need_lock) {
        rw_lock_.ReadUnlock();
    }
  }
};
struct HashTable {
  std::vector<BucketHeader> buckets_;
  HashTable(size_t n_buckets) : buckets_(n_buckets) {};

  bool Add(MyKeyHandle* handle, const MemTableRep::KeyComparator& comparator) {
    return GetBucket(handle->key_, comparator)->Add(handle, comparator);
  }
  bool Contains(const char* key, const MemTableRep::KeyComparator& comparator,
                bool need_lock) const {
    return GetBucket(key, comparator)->Contains(key, comparator, need_lock);
  }
  void Get(const LookupKey& key, const MemTableRep::KeyComparator& comparator,
           void* callback_args,
           bool (*callback_func)(void* arg, const char* entry),
           bool need_lock) {
    GetBucket(key.internal_key(), comparator)->Get(key, comparator, callback_args, callback_func, need_lock);
  }
 private:
  static Slice UserKeyWithoutTimestamp(
      const Slice internal_key, const MemTableRep::KeyComparator& compare) {
    auto key_comparator = static_cast<const MemTable::KeyComparator*>(&compare);
    const Comparator* user_comparator =
        key_comparator->comparator.user_comparator();
    const size_t ts_sz = user_comparator->timestamp_size();
    return ExtractUserKeyAndStripTimestamp(internal_key, ts_sz);
  }
  BucketHeader* GetBucket(const char* key,
                          const MemTableRep::KeyComparator& comparator) const {
    return GetBucket(comparator.decode_key(key), comparator);
  }

  BucketHeader* GetBucket(const Slice& internal_key,
                          const MemTableRep::KeyComparator& comparator) const {
    const size_t hash =
        GetHash(UserKeyWithoutTimestamp(internal_key, comparator));
    BucketHeader* bucket =
        const_cast<BucketHeader*>(&buckets_[hash % buckets_.size()]);
    return bucket;
  }
  static size_t GetHash(const Slice& user_key_without_ts) {
    return MurmurHash(user_key_without_ts.data(),
                      static_cast<int>(user_key_without_ts.size()), 0);
  }
};
class Vector {
 public:
  using Vec = std::vector<const char*>;
  using Iterator = Vec::iterator;

  Vector(size_t limit) : Vector(Vec(limit), 0){}
  Vector(Vec items, size_t n)
      : items_(std::move(items)),
        n_elements_(std::min(n, items.size())),
        sorted_(n_elements_ > 0) {}

  void SetVectorListIter(std::list<std::shared_ptr<Vector>>::iterator list_iter) {
    iter_ = list_iter;
  }
  std::list<std::shared_ptr<Vector>>::iterator GetVectorListIter() {
    return iter_;
  }
  bool Add(const char* key);
  bool IsEmpty() const { return n_elements_ == 0; }
  bool Sort(const MemTableRep::KeyComparator& comparator);
  Iterator SeekForward(const MemTableRep::KeyComparator& comparator, const Slice* seek_key);
  Iterator SeekBackward(const MemTableRep::KeyComparator& comparator, const Slice* seek_key);
  Iterator Seek(const MemTableRep::KeyComparator& comparator, const Slice* slice_key, bool up_direction);
  bool Valid(const Iterator& iter) { return iter != items_.end(); }
  bool Next(Iterator& iter) {
    ++iter;
    return Valid(iter);
  }
  bool Prev(Iterator& iter) {
    if (iter == items_.begin()) {
        iter = items_.end();
        return false;
    }
    --iter;
    return true;
  }
  size_t Size() const { return n_elements_; }
  Iterator End() { return items_.end(); }
 private:
  Vec items_;
  std::atomic<size_t> n_elements_;
  std::atomic<bool> sorted_;
  std::list<std::shared_ptr<Vector>>::iterator iter_;
  port::RWMutex add_rwlock_;
};
bool Vector::Add(const char* key) {
  ReadLock rl(&add_rwlock_);
  if (sorted_) {
    return false;
  }
  const size_t location = n_elements_.fetch_add(1, std::memory_order_relaxed);
  if (location < items_.size()) {
    items_[location] = key;
    return true;
  }
  return false;
}
bool Vector::Sort(const MemTableRep::KeyComparator& comparator) {
  if (sorted_.load(std::memory_order_acquire)) {
    return true;
  }
  WriteLock wl(&add_rwlock_);
  if (n_elements_ == 0) {
    return false;
  }
  if (sorted_.load(std::memory_order_relaxed)) {
    return true;
  }
  const size_t num = std::min(n_elements_.load(), items_.size());
  n_elements_.store(num);
  if (n_elements_ < items_.size()) {
    items_.resize(n_elements_);
  }
  std::sort(items_.begin(), items_.end(), stl_wrappers::Compare(comparator));
  sorted_.store(true, std::memory_order_release);
  return true;
}
Vector::Iterator Vector::SeekForward(const MemTableRep::KeyComparator& comparator, const rocksdb::Slice* seek_key) {
  if (seek_key == nullptr || comparator(items_.front(), *seek_key) >= 0) {
    return items_.begin();
  } else if (comparator(items_.back(), *seek_key) >= 0) {
    return std::lower_bound(items_.begin(), items_.end(), *seek_key, stl_wrappers::Compare(comparator));
  }
  return items_.end();
}
Vector::Iterator Vector::SeekBackward(const MemTableRep::KeyComparator& comparator, const rocksdb::Slice* seek_key) {
  if (seek_key == nullptr || comparator(items_.back(), *seek_key) <= 0) {
    return std::prev(items_.end());
  } else if (comparator(items_.front(), *seek_key) <= 0) {
    auto ret = std::lower_bound(items_.begin(), items_.end(), *seek_key, stl_wrappers::Compare(comparator));
    if (comparator(*ret, *seek_key) >0) {
        --ret;
    }
    return ret;
  }
  return items_.end();
}
Vector::Iterator Vector::Seek(const MemTableRep::KeyComparator& comparator, const rocksdb::Slice* slice_key, bool up_direction) {
  if (!IsEmpty()) {
    if (up_direction) {
        return SeekForward(comparator, slice_key);
    } else {
        return SeekBackward(comparator, slice_key);
    }
  }
  return items_.end();
}

using VectorPtr = std::shared_ptr<Vector>;

class SortHeapItem {
 public:
  SortHeapItem() : vector_(0) {}
  SortHeapItem(VectorPtr vector, Vector::Iterator cur_iter)
      : vector_(vector), cur_iter_(cur_iter) {}
  bool Valid() const { return vector_ && vector_->Valid(cur_iter_); }
  const char* Key() const { return *cur_iter_; }
  bool Next() { return vector_->Next(cur_iter_); }
  bool Prev() { return vector_->Prev(cur_iter_); }

 public:
  VectorPtr vector_;
  Vector::Iterator cur_iter_;
};
class IteratorComparator {
 public:
  IteratorComparator(const MemTableRep::KeyComparator& comparator, bool up_direction)
      : comparator_(comparator), up_direction_(up_direction) {}
  bool operator()(const SortHeapItem* a, const SortHeapItem* b) const {
    return ((up_direction_) ? (comparator_(a->Key(), b->Key()) >  0)
                            : (comparator_(a->Key(), b->Key()) < 0));
  }
  void SetDirection(bool up_direction) { up_direction_ = up_direction; }

 private:
  const MemTableRep::KeyComparator& comparator_;
  bool up_direction_;
};

using IterHeap = BinaryHeap<SortHeapItem*, IteratorComparator>;

class IterHeapInfo {
 public:
  IterHeapInfo(const MemTableRep::KeyComparator& comparator)
      : comparator_(comparator) {}

  const char* Key() const {
    if (iter_heap_.get()->size()!=0) {
      return iter_heap_.get()->top()->Key();
    }
    return nullptr;
  }
  void Reset(bool up_direction) {
    iter_heap_.reset(new IterHeap(IteratorComparator(comparator_, up_direction)));
  }
  bool Valid() const { return iter_heap_.get()->size() != 0;}
  SortHeapItem* Get() {
    if (!Valid()) {
      return nullptr;
    }
    return iter_heap_.get()->top();
  }
  void Update(SortHeapItem* sort_item) {
    if (sort_item->Valid()) {
      iter_heap_.get()->replace_top(sort_item);
    } else {
      iter_heap_.get()->pop();
    }
  }
  void Insert(SortHeapItem* sort_item) { iter_heap_.get()->push(sort_item); }

 private:
  std::unique_ptr<IterHeap> iter_heap_;
  const MemTableRep::KeyComparator& comparator_;
};
using IterAnchors = std::list<SortHeapItem*>;
class VectorContainer {
 public:
  VectorContainer(const MemTableRep::KeyComparator& comparator)
    : comparator_(comparator), switch_vector_limit_(10000),
      num_elements_(0), immutable_(false){
    VectorPtr vector(new Vector(switch_vector_limit_));
    vectors_.push_front(vector);
    vector->SetVectorListIter(std::prev(vectors_.end()));
    cur_vector_.store(vector.get());
    sort_thread_ = port::Thread(&VectorContainer::SortThread, this);
  }
  ~VectorContainer() {
    MarkReadOnly();
    sort_thread_.join();
  }

  void Insert(const char* key);
  bool InternalInsert(const char* key);
  bool IsEmpty() const { return num_elements_.load() == 0; }
  bool IsReadOnly() const { return immutable_.load(); }
  void MarkReadOnly() {
    {
      std::unique_lock<std::mutex> l(sort_thread_mutex_);
      WriteLock wl(&vectors_add_rwlock_);
      immutable_.store(true);
    }
    sort_thread_cv_.notify_one();
  }

  const MemTableRep::KeyComparator& GetComparator() const { return comparator_; }
  void InitIterator(IterAnchors& iter_anchors);


 private:
  void SortThread();

  port::RWMutex vectors_add_rwlock_;
  port::Mutex vectors_mutex_;
  std::list<VectorPtr> vectors_;
  std::atomic<Vector*> cur_vector_;
  const MemTableRep::KeyComparator& comparator_;
  const size_t switch_vector_limit_;
  std::atomic<size_t> num_elements_;
  std::atomic<bool> immutable_;
  port::Thread sort_thread_;
  std::mutex sort_thread_mutex_;
  std::condition_variable sort_thread_cv_;
};
void VectorContainer::SortThread() {
  std::unique_lock<std::mutex> lck(sort_thread_mutex_);
  std::list<VectorPtr>::iterator sort_iter_anchor = vectors_.begin();

  while (1) {
    sort_thread_cv_.wait(lck);
    if (immutable_) {
      break;
    }
    std::list<VectorPtr>::iterator last;
    last = std::prev(vectors_.end());
    if (last == sort_iter_anchor) {
        continue;
    }
    for (; sort_iter_anchor != last; ++sort_iter_anchor) {
        (*sort_iter_anchor)->Sort(comparator_);
    }
  }
}
bool VectorContainer::InternalInsert(const char* key) {
  return cur_vector_.load()->Add(key);
}
void VectorContainer::Insert(const char* key) {
  num_elements_.fetch_add(1, std::memory_order_relaxed);
  {
    ReadLock rl(&vectors_add_rwlock_);
    if (InternalInsert(key)) {
         return;
    }
  }
  bool notify_sort_thread = false;
  {
    WriteLock wl(&vectors_add_rwlock_);
    if (InternalInsert(key)) {
         return;
    }
    {
         MutexLock l(&vectors_mutex_);
         VectorPtr vector(new Vector(switch_vector_limit_));
         vectors_.push_back(vector);
         vector->SetVectorListIter(std::prev(vectors_.end()));
         cur_vector_.store(vector.get());
    }
    notify_sort_thread = true;
    InternalInsert(key);
  }
  if (notify_sort_thread) {
    sort_thread_cv_.notify_one();
  }
}
void VectorContainer::InitIterator(rocksdb::IterAnchors& iter_anchors) {
  if (IsEmpty()) {
    return;
  }
  bool immutable = immutable_.load();
  auto last_iter = cur_vector_.load()->GetVectorListIter();
  bool notify_sort_thread = false;
  if (!immutable) {
    if (!(*last_iter)->IsEmpty()) {
         {
          MutexLock l(&vectors_mutex_);
          VectorPtr vector(new Vector(switch_vector_limit_));
          vectors_.push_back(vector);
          vector->SetVectorListIter(std::prev(vectors_.end()));
          cur_vector_.store(vector.get());
         }
         notify_sort_thread = true;
    } else {
         --last_iter;
    }
  }
  ++last_iter;
  for (auto iter = vectors_.begin(); iter != last_iter; ++iter) {
    SortHeapItem* item = new SortHeapItem(*iter, (*iter)->End());
    iter_anchors.push_back(item);
  }
  if (notify_sort_thread) {
    sort_thread_cv_.notify_one();
  }
}

class VectorIterator : public MemTableRep::Iterator {
 public:
  VectorIterator(std::shared_ptr<VectorContainer> vectors_container,
                 const MemTableRep::KeyComparator& comparator)
    : vectors_container_holder_(vectors_container),
        iter_heap_info_(comparator),
        comparator_(comparator), up_direction_(true),
      is_empty_(true){
    vectors_container_holder_->InitIterator(iter_anchor_);
  }
  ~VectorIterator() override {
    for (auto* item : iter_anchor_) {
         delete item;
    }
  }
  bool Valid() const override {
    return (is_empty_) ? false : iter_heap_info_.Valid();
  }
  bool IsEmpty() { return is_empty_; }
  const char* key() const override {
    return (is_empty_ ? nullptr : iter_heap_info_.Key());
  }
  void Advance() {
    if (!is_empty_) {
         SortHeapItem* sort_item = iter_heap_info_.Get();
         if (up_direction_) {
          sort_item->Next();
         } else {
          sort_item->Prev();
         }
         iter_heap_info_.Update(sort_item);
    }
  }
  void Next() override {
    if (!is_empty_) {
         if (!up_direction_) {
          up_direction_ = true;
         }
         Advance();
    }
  }
  void Prev() override {
     if (!is_empty_) {
         if (up_direction_) {
            up_direction_ = false;
         }
         Advance();
     }
  }
  void SeekInternal(const Slice* seek_key) {
     for (auto const& iter : iter_anchor_) {
         if (iter->vector_->Sort(comparator_)) {
            iter->cur_iter_ = iter->vector_->Seek(comparator_, seek_key, up_direction_);
            if (iter->Valid()) {
              iter_heap_info_.Insert(iter);
            }
         }
     }
  }
  void Seek(const Slice& internal_key, const char* /*memtable_key*/) override {
     if (!is_empty_) {
         up_direction_ = true;
         iter_heap_info_.Reset(true);
         SeekInternal(&internal_key);
     }
  }
  void SeekForPrev(const Slice& internal_key, const char* /*memtable_key*/) override {
     if (!is_empty_) {
         up_direction_ = false;
         iter_heap_info_.Reset(false);
         SeekInternal(&internal_key);
     }
  }
  void SeekToFirst() override {
     if (!is_empty_) {
         up_direction_ = true;
         iter_heap_info_.Reset(true);
         SeekInternal(nullptr);
     }
  }
  void SeekToLast() override {
    if (!is_empty_) {
      up_direction_ = false;
      iter_heap_info_.Reset(false);
      SeekInternal(nullptr);
    }
  }

 private:
  std::shared_ptr<VectorContainer> vectors_container_holder_;
  IterAnchors iter_anchor_;
  IterHeapInfo iter_heap_info_;
  const MemTableRep::KeyComparator& comparator_;
  bool up_direction_;
  bool is_empty_;
};
class HashSortedVectorRep : public MemTableRep {
 public:
  HashSortedVectorRep(const KeyComparator& comparator, Allocator* allocator,
                      size_t num);

  HashSortedVectorRep(Allocator* allocator, size_t bucket_size);
  void Insert(KeyHandle handle) override { InsertKey(handle); }
  void InsertConcurrently(KeyHandle handle) override {
    InsertKey(handle);
  }
  bool InsertKeyConcurrently(KeyHandle handle) override {
    return InsertKey(handle);
  }
  bool InsertKeyWithHintConcurrently(KeyHandle handle, void**) override {
    return InsertKey(handle);
  }
  bool InsertKey(KeyHandle handle) override;
  bool Contains(const char* key) const override;
  void MarkReadOnly() override {
    if (vector_container_) {
      vector_container_->MarkReadOnly();
    }
  };
  size_t ApproximateMemoryUsage() override { return 0; };
  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;
  ~HashSortedVectorRep() override {
    MarkReadOnly();
  }

  MemTableRep::Iterator* GetIterator(Arena* arena) override;

  KeyHandle Allocate(const size_t len, char** buf) override;

 private:
  HashTable hash_table_;
  std::shared_ptr<VectorContainer> vector_container_;
};

HashSortedVectorRep::HashSortedVectorRep(const rocksdb::MemTableRep::KeyComparator& comparator, rocksdb::Allocator* allocator, size_t bucket_size)
  : HashSortedVectorRep(allocator, bucket_size){
  vector_container_ = std::make_shared<VectorContainer>(comparator);
}
HashSortedVectorRep::HashSortedVectorRep(Allocator* allocator, size_t bucket_size)
  : MemTableRep(allocator), hash_table_(bucket_size) {}

KeyHandle HashSortedVectorRep::Allocate(const size_t len, char** buf) {
  size_t alloc_size = sizeof(MyKeyHandle) + len;
  MyKeyHandle* h = reinterpret_cast<MyKeyHandle*>(allocator_->AllocateAligned(alloc_size));
  *buf = h->key_;
  return h;
}

bool HashSortedVectorRep::InsertKey(KeyHandle handle) {
  MyKeyHandle* my_handle = static_cast<MyKeyHandle*>(handle);
  if (!hash_table_.Add(my_handle, vector_container_->GetComparator())) {
    return false;
  }
  vector_container_->Insert(my_handle->key_);
  return true;
}
bool HashSortedVectorRep::Contains(const char* key) const {
  return hash_table_.Contains(key, vector_container_->GetComparator(), !vector_container_->IsReadOnly());
}
void HashSortedVectorRep::Get(const rocksdb::LookupKey& k, void* callback_args, bool (*callback_func)(void*, const char*)) {
  if (vector_container_->IsEmpty()) {
    return;
  }
  hash_table_.Get(k,vector_container_->GetComparator(), callback_args, callback_func, !vector_container_->IsReadOnly());
}
MemTableRep::Iterator* HashSortedVectorRep::GetIterator(rocksdb::Arena* arena) {
  if (arena != nullptr) {
    void* mem = arena->AllocateAligned(sizeof(VectorIterator));
    return new (mem) VectorIterator(vector_container_, vector_container_->GetComparator());
  }
  return new VectorIterator(vector_container_, vector_container_->GetComparator());
}

struct HashSortedVectorRepOptions {
  static const char* kName() { return "HashSortedVectorRepOptions"; }
  size_t hash_bucket_count;
};

static std::unordered_map<std::string, OptionTypeInfo> hash_sortedvector_info = {
    {"hash_bucket_count",
     {offsetof(struct HashSortedVectorRepOptions, hash_bucket_count),
      OptionType::kSizeT, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}}
};

class HashSortedVectorRepFactory : public MemTableRepFactory {
 public:
  explicit HashSortedVectorRepFactory(size_t hash_bucket_count = 1000000) {
    options_.hash_bucket_count = hash_bucket_count;
    RegisterOptions(&options_, &hash_sortedvector_info);
  }
  bool IsInsertConcurrentlySupported() const override { return true; }
  bool CanHandleDuplicatedKey() const override { return true; }
  using MemTableRepFactory::CreateMemTableRep;
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& comparator, Allocator* allocator,
                                 const SliceTransform* , Logger* ) override;

  static const char* kClassName() { return "HashSortedVectorRepFactory"; }
  const char* Name() const override { return kClassName(); }

 private:
  HashSortedVectorRepOptions options_;
};

} // namespace anonymous namespace

MemTableRep* HashSortedVectorRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& comparator,
    rocksdb::Allocator* allocator,
    const rocksdb::SliceTransform* /*transform*/, rocksdb::Logger* /*logger*/) {
  return new HashSortedVectorRep(comparator, allocator, options_.hash_bucket_count);
}
MemTableRepFactory* NewHashSortedVectorRepFactory(size_t bucket_count) {
  return new HashSortedVectorRepFactory(bucket_count);
}

} // namespace ROCKSDB_NAMESPACE