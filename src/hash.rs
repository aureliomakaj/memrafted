use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

pub fn hash(key: &String) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    return hasher.finish();
}
