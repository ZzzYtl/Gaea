package util

import "fmt"

// 栈结构体
type Stack struct {
	// 栈顶
	top int
	// 用slice作容器，定义为interface{}
	data []interface{}
}

// 创建并初始化栈，返回strck
func CreateStack() *Stack {
	s := Stack{}
	s.top = -1
	s.data = make([]interface{}, 0)
	return &s
}

// 判断栈是否为空
func (s *Stack) IsEmpty() bool {
	return s.top == -1
}

// 入栈
func (s *Stack) Push(data interface{}) bool {
	// 栈顶指针+1
	s.top++
	// 把当前的元素放在栈顶的位置
	s.data = append(s.data, data)
	return true
}

// pop,返回栈顶元素
func (s *Stack) Pop() interface{} {
	// 判断是否是空栈
	if s.IsEmpty() {
		return nil
	}
	// 把栈顶的元素赋值给临时变量tmp
	tmp := s.data[s.top]
	s.data = s.data[:s.GetLength()-1]
	// 栈顶指针-1
	s.top--
	return tmp
}

// pop,返回栈顶元素
func (s *Stack) Top() interface{} {
	// 判断是否是空栈
	if s.IsEmpty() {
		return nil
	}
	// 把栈顶的元素赋值给临时变量tmp
	tmp := s.data[s.top]
	return tmp
}

// 栈的元素的长度
func (s *Stack) GetLength() int {
	length := s.top + 1
	return length
}

// 清空栈
func (s *Stack) Clear() {
	s.top = -1
	s.data = make([]interface{}, 0)
}

// 遍历栈
func (s *Stack) Traverse() {
	// 是否为空栈
	if s.IsEmpty() {
		fmt.Println("stack is empty")
	}

	for i := 0; i <= s.top; i++ {
		fmt.Println(s.data[i], " ")
	}
}
