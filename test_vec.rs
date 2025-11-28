use vortexlake_ingest::Document;
use std::collections::HashMap;

fn main() {
    let mut documents = Vec::new();
    documents.push(Document {
        id: "test".to_string(),
        content: "test content".to_string(),
        metadata: HashMap::new(),
    });
    
    // Test indexing
    let first = &documents[0];
    println!("First document ID: {}", first.id);
    
    // Test get method
    if let Some(doc) = documents.get(0) {
        println!("Document via get(): {}", doc.id);
    }
    
    // Test len
    println!("Vector length: {}", documents.len());
    
    println!("Vec operations work correctly!");
}
