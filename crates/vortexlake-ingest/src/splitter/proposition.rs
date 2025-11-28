// src/splitter/proposition_splitter.rs

use crate::models::{Chunk, ChunkMetadata, Document, Granularity};
use crate::splitter::{DocumentSplitter, SplitterConfig};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Utc;
use text_splitter::{TextSplitter};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PropositionSplitter {
    config: SplitterConfig,
}

impl PropositionSplitter {
    pub fn new(config: SplitterConfig) -> Self {
        Self { config }
    }

    /// 快速规则分割（默认兜底，< 5ms）
    fn split_sentences_fast(&self, text: &str) -> Vec<String> {
        let lang = whatlang::detect(text)
            .map(|d| d.lang())
            .unwrap_or(whatlang::Lang::Eng);

        let splitter = TextSplitter::new(4096); // Simple character-based splitting

        let sentences: Vec<String> = splitter
            .chunks(text)
            .map(|s| {
                let mut s = s.trim().to_string();
                if s.is_empty() {
                    return s;
                }
                // 统一加句号，美观 + embedding 更稳定
                if !".!?。！？;；".chars().any(|c| s.ends_with(c)) {
                    if lang == whatlang::Lang::Cmn {
                        s.push('。');
                    } else {
                        s.push('.');
                    }
                }
                s
            })
            .filter(|s| s.chars().count() > 8) // 过滤噪音
            .collect();

        // 短句合并（< 40 字的合并到前一句）
        let mut merged = Vec::new();
        let mut buffer = String::new();

        for sent in sentences {
            if buffer.is_empty() {
                buffer = sent;
            } else if buffer.chars().count() < 40 && sent.chars().count() < 100 {
                buffer = format!("{} {}", buffer.trim_end_matches('.').trim_end_matches('。'), sent);
            } else {
                merged.push(buffer);
                buffer = sent;
            }
        }
        if !buffer.is_empty() {
            merged.push(buffer);
        }

        merged
    }

    /// LLM 原子命题抽取（最强 Recall）
    async fn extract_with_llm(&self, _text: &str) -> Result<Vec<String>> {
        // 你可以自由替换为 ollama / openai / 通义千问 / claude 等
        #[cfg(feature = "ollama")]
        {
            use ollama_rs::{Ollama, generation::completion::GenerationRequest};
            let ollama = Ollama::default();

            let prompt = format!(
                r#"请将以下文本拆解为若干条【独立、可验证的原子事实】，每条必须是完整的句子。
要求：
1. 只保留客观事实，去掉推测、引导词、总结性语句（如“研究表明”“本文提出”等）
2. 每条不超过 120 个字
3. 用中文输出（如果原文是中文）或英文（如果原文是英文）

文本：
{_text}

请以 JSON 数组形式返回，例如：
["张三出生于1990年。", "张三毕业于清华大学。", "该公司2023年营收为10亿元。"]"#
            );

            let resp = ollama
                .generate(GenerationRequest::new("qwen2.5:14b".to_string(), prompt))
                .await?
                .response;

            // 简单提取 JSON 数组
            let start = resp.find('[').ok_or_else(|| anyhow!("No JSON array"))?;
            let end = resp.rfind(']').ok_or_else(|| anyhow!("No closing ]"))?;
            let json = &resp[start..=end];
            let props: Vec<String> = serde_json::from_str(json)?;
            Ok(props)
        }

        #[cfg(not(feature = "ollama"))]
        {
            Err(anyhow!("LLM feature not enabled"))
        }
    }
}

#[async_trait]
impl DocumentSplitter for PropositionSplitter {
    async fn split(&self, document: Document) -> Result<Vec<Chunk>> {
        let text = &document.content;

        // 1. 智能选择策略
        let propositions = if text.len() < 1500 || !self.config.enable_llm_propositions.unwrap_or(false) {
            // 短文本或关闭 LLM → 极速规则分割
            self.split_sentences_fast(text)
        } else {
            // 长文档 + 开了 LLM → 尝试原子命题抽取，失败降级
            match self.extract_with_llm(text).await {
                Ok(props) if props.len() > 3 => props,
                _ => {
                    log::warn!("LLM proposition failed, fallback to rule-based");
                    self.split_sentences_fast(text)
                }
            }
        };

        // 2. 构建 chunks
        let total_chunks = propositions.len();
        let mut chunks = Vec::with_capacity(total_chunks);

        for (i, prop) in propositions.into_iter().enumerate() {
            let mut custom = document.metadata.clone();
            custom.insert("splitter".to_string(), "proposition".into());
            custom.insert("is_atomic_fact".to_string(), true.into());
            custom.insert("proposition_index".to_string(), i.into());

            let metadata = ChunkMetadata {
                title: None,
                section_hierarchy: vec![],
                page_number: None,
                keywords: vec![],
                summary: None,
                is_proposition: true,
                parent_chunk_id: None,
                language: whatlang::detect(text)
                    .map(|d| d.lang().to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                created_at: Utc::now(),
                custom_fields: custom,
            };

            chunks.push(Chunk {
                id: Uuid::new_v4(),
                doc_id: document.source_id.clone(),
                content: prop,
                embedding: None,
                metadata,
                chunk_index: i,
                total_chunks,
                granularity: Granularity::Small,
            });
        }

        Ok(chunks)
    }

    fn name(&self) -> &str {
        "proposition"
    }

    fn config(&self) -> SplitterConfig {
        self.config.clone()
    }
}