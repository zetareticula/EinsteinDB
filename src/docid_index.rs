//! Advanced document ID indexing system with salience and conjugacy factors
//! for optimized search and retrieval in EinsteinDB.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

/// Represents a document with its ID and associated metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: String,
    pub content: String,
    pub metadata: HashMap<String, String>,
}

/// Salience factor for document tokens
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SalienceFactor(f32);

/// Conjugacy factor for token relationships
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ConjugacyFactor(f32);

/// Represents a token in the document with position and salience
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    pub positions: Vec<usize>,
    pub salience: SalienceFactor,
    pub conjugacy: ConjugacyFactor,
}

/// Advanced document index with support for salience and conjugacy factors
pub struct DocumentIndex {
    // Document storage
    documents: RwLock<HashMap<String, Document>>,
    
    // Inverted index: token -> (doc_id -> TokenInfo)
    index: RwLock<HashMap<String, HashMap<String, TokenInfo>>>,
    
    // Document norms for scoring
    norms: RwLock<HashMap<String, f32>>,
}

impl DocumentIndex {
    /// Create a new document index
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            documents: RwLock::new(HashMap::new()),
            index: RwLock::new(HashMap::new()),
            norms: RwLock::new(HashMap::new()),
        })
    }
    
    /// Add or update a document in the index
    pub fn index_document(&self, doc: Document) {
        let doc_id = doc.id.clone();
        let tokens = self.tokenize(&doc.content);
        
        // Calculate document norm (Euclidean length of the document vector)
        let mut norm_squared = 0.0;
        let mut token_info_map = HashMap::new();
        
        for (pos, token) in tokens.iter().enumerate() {
            let entry = token_info_map.entry(token.clone())
                .or_insert_with(|| TokenInfo {
                    positions: Vec::new(),
                    salience: SalienceFactor(1.0), // Default salience
                    conjugacy: ConjugacyFactor(1.0), // Default conjugacy
                });
                
            entry.positions.push(pos);
            norm_squared += 1.0; // Simple term frequency for now
        }
        
        // Update the inverted index
        let mut index = self.index.write();
        for (token, info) in token_info_map {
            index.entry(token)
                .or_default()
                .insert(doc_id.clone(), info);
        }
        
        // Store the document and its norm
        self.documents.write().insert(doc_id.clone(), doc);
        self.norms.write().insert(doc_id, norm_squared.sqrt());
    }
    
    /// Search for documents containing the query terms
    pub fn search(&self, query: &str, top_k: usize) -> Vec<(String, f32)> {
        let tokens = self.tokenize(query);
        let mut scores = HashMap::new();
        
        let index = self.index.read();
        let norms = self.norms.read();
        
        for token in tokens {
            if let Some(doc_entries) = index.get(&token) {
                for (doc_id, token_info) in doc_entries {
                    let score = scores.entry(doc_id.clone()).or_insert(0.0);
                    // Simple TF-IDF like scoring with salience and conjugacy factors
                    *score += token_info.salience.0 * token_info.conjugacy.0 * 
                             (token_info.positions.len() as f32);
                }
            }
        }
        
        // Normalize scores by document length
        let mut results: Vec<_> = scores.into_iter()
            .filter_map(|(doc_id, score)| {
                norms.get(&doc_id).map(|norm| (doc_id, score / norm))
            })
            .collect();
            
        // Sort by score in descending order
        results.sort_unstable_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
        
        // Return top K results
        results.into_iter().take(top_k).collect()
    }
    
    /// Tokenize text into terms (simple whitespace tokenizer for demonstration)
    fn tokenize(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split_whitespace()
            .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()))
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect()
    }
    
    /// Generate a stable document ID from content
    pub fn generate_doc_id(content: &str) -> String {
        let mut hasher = DefaultHasher::new();
        hasher.write(content.as_bytes());
        format!("{:x}", hasher.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_document_indexing() {
        let index = DocumentIndex::new();
        
        let doc1 = Document {
            id: "doc1".to_string(),
            content: "EinsteinDB is a high-performance database".to_string(),
            metadata: HashMap::new(),
        };
        
        let doc2 = Document {
            id: "doc2".to_string(),
            content: "This is a test document about EinsteinDB".to_string(),
            metadata: HashMap::new(),
        };
        
        index.index_document(doc1);
        index.index_document(doc2);
        
        let results = index.search("EinsteinDB database", 10);
        assert!(!results.is_empty());
        assert_eq!(results[0].0, "doc1"); // doc1 should rank higher for this query
    }
}
