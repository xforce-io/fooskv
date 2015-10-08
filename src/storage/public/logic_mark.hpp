#pragma once

namespace xforce { namespace fooskv {

template <typename TypeMark>
class LogicMark {
 public:
  bool Init(const std::string& dir, const std::string& filename);

  void Set(const TypeMark& mark) { mark_ = mark; }
  inline const TypeMark& Get() const { return mark_; }

  bool Save() { Save(mark_); }
  inline bool Save(const TypeMark& mark);
  inline bool Restore();
  inline const TypeMark* RestoreAndGet() { return Restore() ? &mark_ : NULL; }
  
 private:
  string filepath_;
  TypeMark mark_; 
};

template <typename TypeMark>
bool LogicMark<TypeMark>::Init(const std::string& dir, const std::string& filename) {
  std::stringstream ss;
  ss << dir << "/" << filename;
  filepath_ = ss.str();

  ss.str("");
  ss << "mkdir -p " << dir;
  int ret = system(ss.str());
  if (0!=ret) {
    WARN("fail_exec[" << ss.str() << "]");
    return false;
  }
  return true;
}

template <typename TypeMark>
bool LogicMark<TypeMark>::Save(const TypeMark& mark) {
  FILE* fp = fopen(filepath_.c_str(), "w");
  if (NULL==fp) {
    WARN("fail_open_for_write[" << filepath_ << "]");
    return false;
  }
  return mark.Serialize(fp);
}

template <typename TypeMark>
bool LogicMark<TypeMark>::Restore() {
  FILE* fp = fopen(filepath_.c_str(), "w");
  if (NULL==fp) {
    WARN("fail_open_for_write[" << filepath_ << "]");
    return false;
  }
  return mark_.Deserialize(fp);
}

}}
