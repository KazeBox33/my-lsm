#include "skiplist/skiplist.h"
#include <cstdint>
#include <iostream>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <tuple>
#include <utility>

namespace tiny_lsm {

// ************************ SkipListIterator ************************
BaseIterator &SkipListIterator::operator++() {
  // TODO: Lab1.2 任务：实现SkipListIterator的++操作符
  // ? current 是当前节点指针, forward_[0] 是最底层链表的下一个节点
  if(current){
    current=current->forward_[0];
  }
  return *this;
}

bool SkipListIterator::operator==(const BaseIterator &other) const {
  // TODO: Lab1.2 任务：实现SkipListIterator的==操作符
  // ? 需要先通过 get_type() 判断类型再做 dynamic_cast
  if(other.get_type()!=IteratorType::SkipListIterator){
    return false;
  }
  const auto &other_iter=dynamic_cast<const SkipListIterator &>(other);
  return current==other_iter.current;
}

bool SkipListIterator::operator!=(const BaseIterator &other) const {
  // TODO: Lab1.2 任务：实现SkipListIterator的!=操作符
  return !(*this==other);
}

SkipListIterator::value_type SkipListIterator::operator*() const {
  // TODO: Lab1.2 任务：实现SkipListIterator的*操作符
  // ? 若 current 为空需抛出异常
  if(current==nullptr){
    throw std::out_of_range("SkipListIterator is invalid!!");
  }
  return {get_key(), get_value()};
}

IteratorType SkipListIterator::get_type() const {
  // TODO: Lab1.2 任务：实现SkipListIterator的get_type
  // ? 主要是为了熟悉基类的定义和继承关系, 返回 IteratorType::SkipListIterator
  return IteratorType::SkipListIterator; // placeholder, 请替换为正确实现
}

bool SkipListIterator::is_valid() const {
  return current && !current->key_.empty();
}
bool SkipListIterator::is_end() const { return current == nullptr; }

std::string SkipListIterator::get_key() const { return current->key_; }
std::string SkipListIterator::get_value() const { return current->value_; }
uint64_t SkipListIterator::get_tranc_id() const { return current->tranc_id_; }

// ************************ SkipList ************************
// 构造函数
SkipList::SkipList(int max_lvl) : max_level(max_lvl), current_level(1) {
  head = std::make_shared<SkipListNode>("", "", max_level, 0);
  dis_01 = std::uniform_int_distribution<>(0, 1);
  dis_level = std::uniform_int_distribution<>(0, (1 << max_lvl) - 1);
  gen = std::mt19937(std::random_device()());
}

int SkipList::random_level() {
  // ? 通过"抛硬币"的方式随机生成层数：
  // ? - 每次有50%的概率增加一层
  // ? - 确保层数分布为：第1层100%，第2层50%，第3层25%，以此类推
  // ? - 层数范围限制在[1, max_level]之间，避免浪费内存
  // TODO: Lab1.1 任务：插入时随机为这一次操作确定其最高连接的链表层数
  int level = 1;
  while(level<max_level){
    int coin=dis_01(gen);
    if(coin==0){
      break;
    }
    level+=1;
  }
  return level;
}

// 插入或更新键值对
void SkipList::put(const std::string &key, const std::string &value,
                   uint64_t tranc_id) {
  spdlog::trace("SkipList--put({}, {}, {})", key, value, tranc_id);

  // TODO: Lab1.1 任务：实现插入或更新键值对
  // ? Hint: 你需要保证不同`Level`的步长从底层到高层逐渐增加
  // ? 你可能需要使用到`random_level`函数以确定层数, 其注释中为你提供一种思路
  // ? tranc_id 为事务id, 直接将其传递到 SkipListNode 的构造函数中即可
  // ? 若key存在且tranc_id相同, 仅更新value; 否则插入新节点
  // ? 注意维护 size_bytes
   auto cur_node=head;
   int level=current_level-1;
   std::vector<std::shared_ptr<SkipListNode>> update(max_level,head);
  while(level>=0){
    while(cur_node->forward_[level]&&(cur_node->forward_[level]->key_<key||(cur_node->forward_[level]->key_==key&&cur_node->forward_[level]->tranc_id_>tranc_id))){ // 不断向右侧靠去
      cur_node=cur_node->forward_[level];
    }
    update[level]=cur_node;
    level--;
  }
  if(cur_node->forward_[0]!=nullptr&&cur_node->forward_[0]->key_==key&&cur_node->forward_[0]->tranc_id_==tranc_id){
    size_bytes = size_bytes - cur_node->forward_[0]->value_.size() + value.size();
    cur_node->forward_[0]->value_=value;
    return;
  }
  int new_level=random_level();
  auto new_node=std::make_shared<SkipListNode>(key,value,new_level,tranc_id);
  for(int i=0;i<new_level;i++){
    new_node->forward_[i]=update[i]->forward_[i];
    update[i]->forward_[i]=new_node;
    new_node->set_backward(i,update[i]);
    if(new_node->forward_[i]!=nullptr){
    new_node->forward_[i]->set_backward(i,new_node);
    }
  }
  if(new_level>current_level) current_level=new_level;
  size_bytes+=key.size()+value.size()+sizeof(uint64_t);
}

// 查找键值对
SkipListIterator SkipList::get(const std::string &key, uint64_t tranc_id) {
  spdlog::trace("SkipList--get({}) called", key);

  // TODO: Lab1.1 任务：实现查找键值对
  // ? 从最高层开始向下查找, 最终在底层确认 key 是否存在
  // ? 若 tranc_id == 0, 直接比较 key 返回; 否则需满足事务可见性 (tranc_id_ <= tranc_id)
 
  int level=current_level-1;
  auto cur_node=head;
  while(level>=0){
    while(cur_node->forward_[level]&&cur_node->forward_[level]->key_<key){
      cur_node=cur_node->forward_[level];
    }
    level--;
  }
  auto next_node=cur_node->forward_[0];
  if(next_node==nullptr||next_node->key_!=key){
    return SkipListIterator{};
  }
  if(tranc_id==0){ // 如果不开事务,直接返回最新版本
    return SkipListIterator{next_node};
  }

  //否则顺着同key的版本链往后找第一个可见的版本
  while(next_node&&next_node->key_==key){
    if(next_node->tranc_id_<=tranc_id){ // 表示此时这个node对于事务来说是可见的
      return SkipListIterator(next_node);
    }
    next_node=next_node->forward_[0];
  }

  
  // TODO: 完成查找后还需要额外实现SkipListIterator中的TODO部分(Lab1.2)

  return SkipListIterator{}; // 不可见则返回空
}

// 删除键值对
// ! 这里的 remove 是跳表本身真实的 remove,  lsm 应该使用 put 空值表示删除,
// ! 这里只是为了实现完整的 SkipList 不会真正被上层调用
void SkipList::remove(const std::string &key) {
  // TODO: Lab1.1 任务：实现删除键值对
  // ? 从最高层开始查找目标节点并更新各层指针
  // ? 注意同时维护 backward_ 指针和 size_bytes
  std::vector<std::shared_ptr<SkipListNode>> update(max_level,head);
  auto cur_node=head;
  int level=current_level-1;
  //先找到每一层的前驱节点
  while(level>=0){
    while(cur_node->forward_[level]&&cur_node->forward_[level]->key_<key){
      cur_node=cur_node->forward_[level];
    }
    update[level]=cur_node;
    level--;
  }

  auto target=cur_node->forward_[0];

  if(target==nullptr||target->key_!=key){
    //如果不存在 ,直接return;
    return;
  }

  //删除这个key的所有版本;
  while(target&&target->key_==key){
    auto next_node=target->forward_[0];
    for(int i=0;i<target->forward_.size();i++){
      if(update[i]->forward_[i]==target){
        update[i]->forward_[i]=target->forward_[i];
      }
      if(target->forward_[i]){
        target->forward_[i]->set_backward(i,update[i]);
      }
    }
    size_bytes-=target->key_.size()+target->value_.size()+sizeof(uint64_t);
    target=next_node;
  }

  //如果最高层已经空了,降低当前有效层数
  while(current_level>1&&head->forward_[current_level-1]==nullptr){
    current_level--;
  }
  
}

// 刷盘时可以直接遍历最底层链表
std::vector<std::tuple<std::string, std::string, uint64_t>> SkipList::flush() {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);
  spdlog::debug("SkipList--flush(): Starting to flush skiplist data");

  std::vector<std::tuple<std::string, std::string, uint64_t>> data;
  auto node = head->forward_[0];
  while (node) {
    data.emplace_back(node->key_, node->value_, node->tranc_id_);
    node = node->forward_[0];
  }

  spdlog::debug("SkipList--flush(): Flushed {} entries", data.size());

  return data;
}

size_t SkipList::get_size() {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);
  return size_bytes;
}

// 清空跳表，释放内存
void SkipList::clear() {
  // std::unique_lock<std::shared_mutex> lock(rw_mutex);
  head = std::make_shared<SkipListNode>("", "", max_level, 0);
  size_bytes = 0;
}

SkipListIterator SkipList::begin() {
  // return SkipListIterator(head->forward[0], rw_mutex);
  return SkipListIterator(head->forward_[0]);
}

SkipListIterator SkipList::end() {
  return SkipListIterator(); // 使用空构造函数
}

// 找到前缀的起始位置
// 返回第一个前缀匹配或者大于前缀的迭代器
SkipListIterator SkipList::begin_preffix(const std::string &preffix) {
  // TODO: Lab1.3 任务：实现前缀查询的起始位置
  // ? 从最高层开始查找, 找到第一个 key >= preffix 的节点
  return SkipListIterator{};
}

// 找到前缀的终结位置
SkipListIterator SkipList::end_preffix(const std::string &prefix) {
  // TODO: Lab1.3 任务：实现前缀查询的终结位置
  // ? 找到第一个 key 不以 prefix 开头的节点作为终结位置
  return SkipListIterator{};
}

// ? 这里单调谓词的含义是, 整个数据库只会有一段连续区间满足此谓词
// ? 例如之前特化的前缀查询，以及后续可能的范围查询，都可以转化为谓词查询
// ? 返回第一个满足谓词的位置和最后一个满足谓词的迭代器
// ? 如果不存在, 返回 nullopt
// ? 谓词作用于key, 且保证满足谓词的结果只在一段连续的区间内, 例如前缀匹配的谓词
// ? predicate返回值:
// ?   0: 满足谓词
// ?   >0: 不满足谓词, 需要向右移动
// ?   <0: 不满足谓词, 需要向左移动
// ! Skiplist 中的谓词查询不会进行事务id的判断, 需要上层自己进行判断
std::optional<std::pair<SkipListIterator, SkipListIterator>>
SkipList::iters_monotony_predicate(
    std::function<int(const std::string &)> predicate) {
  // TODO: Lab1.3 任务：实现谓词查询
  // ? 分两步: 1. 利用多层跳表快速找到谓词满足区间内的一个节点
  // ?         2. 分别向前/向后扩展, 利用 backward_ 和 forward_ 确定区间边界
  // ? 注意: 向前查找时需要利用 backward_ 指针从当前节点的最高层开始回溯
  return std::nullopt;
}

// ? 打印跳表, 你可以在出错时调用此函数进行调试
void SkipList::print_skiplist() {
  for (int level = 0; level < current_level; level++) {
    std::cout << "Level " << level << ": ";
    auto current = head->forward_[level];
    while (current) {
      std::cout << current->key_;
      current = current->forward_[level];
      if (current) {
        std::cout << " -> ";
      }
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;
}
} // namespace tiny_lsm
