# Rust 调试指南

本文档介绍在 VortexLake 项目中进行 Rust 代码调试的各种方法和技术。

## 目录
1. [基本调试方法](#基本调试方法)
2. [VS Code + rust-analyzer 调试](#vs-code--rust-analyzer-调试)
3. [命令行调试器](#命令行调试器)
4. [测试驱动调试](#测试驱动调试)
5. [性能调试](#性能调试)
6. [常见调试技巧](#常见调试技巧)
7. [Cursor IDE 代码跳转](#cursor-ide-代码跳转)

## 基本调试方法

### 1. println!() 调试

最简单的调试方法，在代码中插入打印语句：

```rust
fn process_data(data: &Vec<i32>) {
    println!("处理数据: {:?}", data); // 打印整个向量

    for (i, &value) in data.iter().enumerate() {
        println!("索引 {}: 值 {}", i, value); // 打印每个元素
    }

    let sum: i32 = data.iter().sum();
    println!("总和: {}", sum); // 打印计算结果
}
```

### 2. dbg!() 宏

`dbg!()` 宏会自动打印文件名、行号和值，非常适合临时调试：

```rust
fn calculate_average(scores: &[f32]) -> f32 {
    let sum = dbg!(scores.iter().sum::<f32>());  // 打印总和
    let count = dbg!(scores.len() as f32);       // 打印数量
    let avg = dbg!(sum / count);                 // 打印平均值
    avg
}

// 输出示例:
// [src/main.rs:5:13] scores.iter().sum::<f32>() = 255.0
// [src/main.rs:6:15] scores.len() as f32 = 5.0
// [src/main.rs:7:13] sum / count = 51.0
```

### 3. assert!() 断言

用于验证代码的正确性，失败时会panic：

```rust
fn process_user(user: &User) {
    assert!(user.id > 0, "用户ID必须大于0");
    assert!(!user.name.is_empty(), "用户名不能为空");
    assert!(user.scores.iter().all(|&s| s >= 0.0 && s <= 100.0),
            "分数必须在0-100之间");

    // 继续处理...
}
```

### 4. 条件编译调试

```rust
// 在 Cargo.toml 中添加:
// [features]
// debug_mode = []

#[cfg(feature = "debug_mode")]
macro_rules! debug_print {
    ($($arg:tt)*) => {
        println!("[DEBUG] {}", format_args!($($arg)*));
    };
}

#[cfg(not(feature = "debug_mode"))]
macro_rules! debug_print {
    ($($arg:tt)*) => {};
}

// 使用:
fn expensive_calculation(data: &Vec<f64>) {
    debug_print!("开始计算，数据长度: {}", data.len());

    // 计算逻辑...

    debug_print!("计算完成，结果: {}", result);
}
```

## VS Code + rust-analyzer 调试

### 设置断点

1. 点击行号左侧设置断点（红色圆点）
2. 使用 `F9` 键切换断点
3. 在代码中点击设置断点

### 启动调试

1. 按 `F5` 或点击侧边栏的调试图标
2. 选择 "Rust" 配置
3. 或者创建 `.vscode/launch.json`：

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug Rust",
            "type": "lldb",
            "request": "launch",
            "program": "${workspaceFolder}/target/debug/vortexlake",
            "args": [],
            "cwd": "${workspaceFolder}",
            "preLaunchTask": "cargo build"
        }
    ]
}
```

### 调试面板功能

- **Variables**: 查看变量值
- **Watch**: 监视特定表达式
- **Call Stack**: 查看调用栈
- **Breakpoints**: 管理所有断点

### 调试控制

- `F5`: 继续执行
- `F10`: 单步执行（不进入函数）
- `F11`: 单步执行（进入函数）
- `Shift+F11`: 跳出函数
- `Ctrl+Shift+F5`: 重启调试

## 命令行调试器

### GDB (Linux/macOS)

```bash
# 编译时包含调试信息
cargo build

# 使用 gdb 调试
gdb target/debug/vortexlake

# gdb 命令
(gdb) break main                    # 在 main 函数设置断点
(gdb) break 42                      # 在第42行设置断点
(gdb) run                           # 运行程序
(gdb) print variable_name           # 打印变量值
(gdb) continue                      # 继续执行
(gdb) step                          # 单步执行
(gdb) next                          # 单步执行（不进入函数）
(gdb) backtrace                     # 查看调用栈
(gdb) quit                          # 退出
```

### LLDB (macOS)

```bash
# 使用 lldb 调试
lldb target/debug/vortexlake

# lldb 命令（类似 gdb）
(lldb) breakpoint set -n main       # 在 main 函数设置断点
(lldb) breakpoint set -l 42         # 在第42行设置断点
(lldb) run                          # 运行程序
(lldb) print variable_name          # 打印变量值
(lldb) continue                     # 继续执行
(lldb) step                         # 单步执行
(lldb) next                         # 单步执行（不进入函数）
(lldb) thread backtrace             # 查看调用栈
```

## 测试驱动调试

### 单元测试

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_logic() {
        // 准备测试数据
        let input = vec![1, 2, 3, 4, 5];
        let expected = 15;

        // 执行测试
        let result = sum(&input);

        // 断言结果
        assert_eq!(result, expected, "求和结果不正确");
    }

    #[test]
    fn test_edge_cases() {
        // 测试边界情况
        assert_eq!(sum(&[]), 0, "空数组求和应该为0");
        assert_eq!(sum(&[42]), 42, "单元素数组求和应该等于元素值");
    }

    #[test]
    #[should_panic(expected = "数组不能为空")]
    fn test_panic_conditions() {
        // 测试应该panic的情况
        dangerous_function(&[]);
    }
}
```

运行测试：
```bash
cargo test                    # 运行所有测试
cargo test test_function      # 运行特定测试
cargo test -- --nocapture     # 显示测试中的println!输出
```

### 集成测试

在 `tests/` 目录下创建集成测试：

```rust
// tests/integration_test.rs
use vortexlake_core::{VortexLake, Schema, Field};

#[test]
fn test_database_operations() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_string_lossy();

    // 创建数据库
    let db = VortexLake::new(&db_path).unwrap();

    // 创建表
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("vector", DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, false)),
            384
        ), false),
    ]).unwrap();

    db.create_table("test", schema).unwrap();

    // 验证表存在
    let tables = db.list_tables();
    assert!(tables.contains(&"test".to_string()));
}
```

### 基准测试

```rust
// 在 Cargo.toml 中添加:
// [[bench]]
// name = "my_benchmark"
// harness = false

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn benchmark_vector_search(c: &mut Criterion) {
    let data = create_test_data(10000);

    c.bench_function("vector_search", |b| {
        b.iter(|| {
            let result = black_box(search_vectors(&data, &query_vector));
            result
        })
    });
}

criterion_group!(benches, benchmark_vector_search);
criterion_main!(benches);
```

运行基准测试：
```bash
cargo bench
```

## 性能调试

### 性能剖析

1. **使用 cargo-flamegraph**:
```bash
cargo install flamegraph
cargo flamegraph --bin my_binary
# 生成火焰图 SVG 文件
```

2. **使用 perf (Linux)**:
```bash
# 记录性能数据
perf record target/release/my_binary

# 生成报告
perf report
```

3. **使用 Instruments (macOS)**:
```bash
# 使用 Xcode Instruments 或命令行工具
```

### 内存调试

1. **使用 Valgrind (Linux)**:
```bash
cargo build --release
valgrind --tool=memcheck target/release/my_binary
```

2. **使用 heaptrack**:
```bash
heaptrack target/release/my_binary
heaptrack_gui heaptrack.my_binary.*
```

## 常见调试技巧

### 1. 调试宏

```rust
macro_rules! trace {
    ($expr:expr) => {{
        let result = $expr;
        eprintln!("[TRACE] {} = {:?}", stringify!($expr), result);
        result
    }};
}

// 使用:
let sum = trace!(a + b);
```

### 2. 条件断点

```rust
// 在代码中添加条件断点
fn process_items(items: &[Item]) {
    for (i, item) in items.iter().enumerate() {
        if i == 42 || item.value > 1000 {  // 条件断点
            // 设置实际断点在这里
            println!("特殊情况: 索引={}, 值={}", i, item.value);
        }
        // 处理逻辑...
    }
}
```

### 3. 日志调试

```rust
use tracing::{info, warn, error};

fn complex_function(data: &ComplexData) {
    info!("开始处理数据: {}", data.id);

    if data.is_valid() {
        info!("数据验证通过");
        // 处理逻辑...
    } else {
        warn!("数据验证失败: {:?}", data);
        return;
    }

    match process_data(data) {
        Ok(result) => info!("处理成功: {}", result),
        Err(e) => error!("处理失败: {}", e),
    }
}
```

### 4. 二分调试

```rust
fn find_buggy_function() {
    // 注释掉一半代码，看看问题是否还存在
    // step1();
    // step2();
    step3();  // 如果问题还存在，bug在这里或之后
    // step4();
    // step5();
}
```

### 5. 最小重现

```rust
fn minimal_reproduction() {
    // 创建最小的测试用例来重现bug
    let minimal_data = vec![1, 2, 3];  // 最小数据集
    let result = buggy_function(&minimal_data);
    assert_eq!(result, expected);  // 应该失败
}
```

## Cursor IDE 代码跳转

### 代码导航

Cursor 支持完整的代码跳转功能，通过 rust-analyzer 插件实现：

#### Go to Definition (转到定义)
- **快捷键**: `F12` 或 `Ctrl+点击`
- **功能**: 跳转到函数、结构体、trait等的定义位置
- **使用**: 将光标放在函数调用上，按 `F12` 跳转到定义

#### Find References (查找引用)
- **快捷键**: `Shift+F12`
- **功能**: 查找所有引用该符号的位置
- **使用**: 选中一个函数名，查看它在哪里被调用

#### Go to Symbol
- **快捷键**: `Ctrl+Shift+O`
- **功能**: 在当前文件中跳转到符号
- **使用**: 快速跳转到结构体字段、函数等

#### Workspace Symbol
- **快捷键**: `Ctrl+T`
- **功能**: 在整个工作区中搜索符号
- **使用**: 查找项目中的任何函数、类型等

### 配置 rust-analyzer

确保 Cursor 中的 rust-analyzer 配置正确：

1. **检查插件**: 确保 rust-analyzer 插件已安装并启用
2. **配置设置**:
   ```json
   {
     "rust-analyzer.checkOnSave.enable": true,
     "rust-analyzer.cargo.loadOutDirsFromCheck": true,
     "rust-analyzer.procMacro.enable": true
   }
   ```

3. **排除文件**: 在 `.vscode/settings.json` 中配置：
   ```json
   {
     "rust-analyzer.files.excludeDirs": [
       "target",
       "node_modules"
     ]
   }
   ```

### 代码跳转示例

```rust
// 在 Cursor 中，将光标放在任一位置：

// 1. 放在函数调用上，按 F12 跳转到定义
let result = process_users(&users);

// 2. 放在结构体名上，按 F12 查看定义
let user = User::new(1, "Alice");

// 3. 放在 trait 方法上，按 F12 查看实现
user.add_score(85.0);

// 4. 放在类型上，按 F12 查看定义
let scores: Vec<f32> = vec![85.0, 90.0];
```

### 故障排除

如果代码跳转不工作：

1. **检查 rust-analyzer 状态**: 查看状态栏的 rust-analyzer 图标
2. **重新加载窗口**: `Ctrl+Shift+P` → "Developer: Reload Window"
3. **检查 Cargo.toml**: 确保依赖正确
4. **运行 cargo check**: 确保代码编译通过
5. **更新 rust-analyzer**: 检查插件是否为最新版本

### 高级功能

- **悬停查看**: 将鼠标悬停在符号上查看类型信息
- **内联提示**: 查看函数参数名和类型
- **代码动作**: `Ctrl+.` 查看可用重构操作
- **Peek Definition**: `Alt+F12` 在侧边栏查看定义

通过这些调试技术和Cursor的代码跳转功能，你可以高效地开发和调试VortexLake项目！
