// Rust调试示例 - 演示各种调试方法
// 使用: cargo run --bin debug_examples

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct User {
    pub id: u32,
    pub name: String,
    pub email: String,
    pub scores: Vec<f32>,
}

impl User {
    pub fn new(id: u32, name: &str, email: &str) -> Self {
        Self {
            id,
            name: name.to_string(),
            email: email.to_string(),
            scores: Vec::new(),
        }
    }

    pub fn add_score(&mut self, score: f32) {
        self.scores.push(score);
    }

    pub fn average_score(&self) -> f32 {
        if self.scores.is_empty() {
            return 0.0;
        }
        self.scores.iter().sum::<f32>() / self.scores.len() as f32
    }

    pub fn has_passing_score(&self, threshold: f32) -> bool {
        // 这里有一个bug：应该检查平均分而不是最后一个分数
        if let Some(last_score) = self.scores.last() {
            *last_score >= threshold
        } else {
            false
        }
    }
}

pub fn process_users(users: &mut Vec<User>) {
    println!("开始处理用户数据...");

    // 模拟一些处理逻辑
    for user in users.iter_mut() {
        // 添加一些随机分数
        for i in 0..5 {
            let score = 60.0 + (user.id as f32 * 2.0) + (i as f32 * 5.0);
            user.add_score(score.min(100.0));
        }

        println!("用户 {}: {} 分数, 平均分: {:.2}",
                 user.name,
                 user.scores.len(),
                 user.average_score());
    }

    // 过滤及格用户 (这里会暴露bug)
    let passing_users: Vec<_> = users.iter()
        .filter(|user| user.has_passing_score(75.0))
        .collect();

    println!("及格用户数量: {}", passing_users.len());

    // 统计分析
    let stats = calculate_stats(users);
    println!("统计信息: {:?}", stats);
}

pub fn calculate_stats(users: &[User]) -> HashMap<String, f32> {
    let mut stats = HashMap::new();

    if users.is_empty() {
        return stats;
    }

    let total_users = users.len() as f32;
    let avg_scores: Vec<f32> = users.iter().map(|u| u.average_score()).collect();
    let overall_avg = avg_scores.iter().sum::<f32>() / avg_scores.len() as f32;

    let highest_avg = avg_scores.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
    let lowest_avg = avg_scores.iter().cloned().fold(f32::INFINITY, f32::min);

    stats.insert("total_users".to_string(), total_users);
    stats.insert("overall_average".to_string(), overall_avg);
    stats.insert("highest_average".to_string(), highest_avg);
    stats.insert("lowest_average".to_string(), lowest_avg);

    stats
}

fn demonstrate_error_handling() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== 错误处理演示 ===");

    // 模拟文件读取
    let data = std::fs::read_to_string("nonexistent_file.txt")
        .map_err(|e| format!("读取文件失败: {}", e))?;

    println!("文件内容: {}", data);
    Ok(())
}

fn main() {
    println!("=== Rust调试示例 ===\n");

    // 创建测试数据
    let mut users = vec![
        User::new(1, "Alice", "alice@example.com"),
        User::new(2, "Bob", "bob@example.com"),
        User::new(3, "Charlie", "charlie@example.com"),
        User::new(4, "David", "david@example.com"),
    ];

    // 设置断点调试点1: 在这里设置断点查看users状态
    println!("创建了 {} 个用户", users.len());

    // 处理用户数据
    process_users(&mut users);

    // 设置断点调试点2: 在这里检查处理结果
    println!("\n最终用户状态:");
    for user in &users {
        println!("{:?}", user);
    }

    // 演示dbg!宏 - 它会打印文件名、行号和值
    let test_value = 42;
    let debugged = dbg!(test_value * 2);
    println!("dbg!返回值: {}", debugged);

    // 演示assert! - 用于调试时的断言检查
    for user in &users {
        assert!(user.id > 0, "用户ID必须大于0");
        assert!(!user.name.is_empty(), "用户名不能为空");
        assert!(user.average_score() >= 0.0 && user.average_score() <= 100.0,
                "平均分必须在0-100之间");
    }
    println!("所有断言检查通过!");

    // 演示错误处理
    if let Err(e) = demonstrate_error_handling() {
        println!("错误演示: {}", e);
    }

    println!("\n=== 调试技巧提示 ===");
    println!("1. 使用println!()进行简单调试");
    println!("2. 使用dbg!()宏查看变量值");
    println!("3. 使用assert!()进行断言检查");
    println!("4. 使用cargo test进行单元测试");
    println!("5. 使用VS Code + rust-analyzer进行图形化调试");
    println!("6. 使用gdb/lldb进行命令行调试");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_creation() {
        let user = User::new(1, "Alice", "alice@example.com");
        assert_eq!(user.id, 1);
        assert_eq!(user.name, "Alice");
        assert_eq!(user.email, "alice@example.com");
        assert!(user.scores.is_empty());
    }

    #[test]
    fn test_add_score_and_average() {
        let mut user = User::new(1, "Alice", "alice@example.com");
        user.add_score(85.0);
        user.add_score(90.0);
        user.add_score(88.0);

        assert_eq!(user.scores.len(), 3);
        assert_eq!(user.average_score(), 87.66666666666667);
    }

    #[test]
    fn test_has_passing_score_bug() {
        let mut user = User::new(1, "Alice", "alice@example.com");

        // 空分数应该返回false
        assert!(!user.has_passing_score(75.0));

        // 添加分数测试 - 这里暴露了bug
        user.add_score(80.0); // 平均分会是78.33
        user.add_score(70.0);
        user.add_score(85.0);

        // Bug: 只检查最后一个分数(85.0 >= 75.0 = true)
        // 但应该检查平均分(78.33 >= 75.0 = true)
        // 这个测试会通过，但逻辑是错的
        assert!(user.has_passing_score(75.0));

        // 正确的检查应该是平均分
        assert!(user.average_score() >= 75.0);
    }

    #[test]
    fn test_calculate_stats() {
        let mut users = vec![
            User::new(1, "Alice", "alice@example.com"),
            User::new(2, "Bob", "bob@example.com"),
        ];

        // 添加分数
        users[0].add_score(80.0);
        users[0].add_score(90.0); // Alice平均: 85.0
        users[1].add_score(70.0);
        users[1].add_score(85.0); // Bob平均: 77.5

        let stats = calculate_stats(&users);

        assert_eq!(stats["total_users"], 2.0);
        assert_eq!(stats["overall_average"], 81.25); // (85.0 + 77.5) / 2
        assert_eq!(stats["highest_average"], 85.0);
        assert_eq!(stats["lowest_average"], 77.5);
    }
}
