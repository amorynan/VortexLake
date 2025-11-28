fn main() {
    let mut documents: Vec<String> = Vec::new();
    documents.push("test document".to_string());
    
    // Test indexing
    let first = &documents[0];
    println!("First document: {}", first);
    
    // Test get method
    if let Some(doc) = documents.get(0) {
        println!("Document via get(): {}", doc);
    }
    
    // Test len
    println!("Vector length: {}", documents.len());
    
    println!("Vec operations work correctly!");
}
