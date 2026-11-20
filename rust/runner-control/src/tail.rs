//! Disposable primary-change history. It never participates in Raft voting.
use crate::control::{n, s};
use serde_json::{Value, json};
use std::collections::BTreeMap;
#[derive(Default)]
pub struct Tail {
    transitions: BTreeMap<String, Value>,
    trimmed_through: Option<(u64, String)>,
}
impl Tail {
    pub fn append(&mut self, kind: &str, data: &Value, _at: u64) -> anyhow::Result<()> {
        if kind == "transition" {
            self.transition(data);
        }
        Ok(())
    }
    pub fn transition(&mut self, data: &Value) {
        let id = s(data, "id");
        let epoch = n(data, "epoch").max(n(data, "term"));
        if id.is_empty()
            || id.len() > 512
            || epoch == 0
            || serde_json::to_vec(data).map_or(true, |v| v.len() > 4096)
        {
            return;
        }
        if self
            .trimmed_through
            .as_ref()
            .is_some_and(|floor| &(epoch, id.to_owned()) <= floor)
        {
            return;
        }
        self.transitions
            .entry(id.to_owned())
            .or_insert_with(|| data.clone());
        self.trim(512); // Emergency memory/transport bound; history can be lost.
    }
    pub fn trim(&mut self, keep: usize) {
        let mut order = self
            .transitions
            .iter()
            .map(|(id, v)| (n(v, "epoch").max(n(v, "term")), id.clone()))
            .collect::<Vec<_>>();
        order.sort();
        let remove = order.len().saturating_sub(keep);
        for key in order.into_iter().take(remove) {
            self.transitions.remove(&key.1);
            if self
                .trimmed_through
                .as_ref()
                .is_none_or(|floor| floor < &key)
            {
                self.trimmed_through = Some(key);
            }
        }
    }
    pub fn history(&self) -> Value {
        let mut rows = self.transitions.values().cloned().collect::<Vec<_>>();
        rows.sort_by_key(|v| {
            std::cmp::Reverse((n(v, "epoch").max(n(v, "term")), s(v, "id").to_owned()))
        });
        json!(rows)
    }
    pub fn read(&self, _after: u64) -> anyhow::Result<Value> {
        Ok(json!({"history":self.history()}))
    }
    pub fn reports(&self) -> anyhow::Result<Vec<Value>> {
        Ok(vec![])
    }
    pub fn stats(&self) -> Value {
        json!({"entries":self.transitions.len(),"transitions":self.transitions.len(),"bytes":self.transitions.values().map(|v|serde_json::to_vec(v).unwrap().len()).sum::<usize>(),"kind":"primary-history","storage":"memory"})
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_primary_changes_are_retained_and_trimming_prevents_reintroduction() {
        let mut tail = Tail::default();
        tail.append("report", &json!({"id":"r","epoch":1}), 0)
            .unwrap();
        tail.append("event", &json!({"id":"e","epoch":1}), 0)
            .unwrap();
        assert_eq!(tail.stats()["entries"], 0);
        for epoch in 1..=130 {
            tail.transition(&json!({"id":format!("primary-{epoch}"),"epoch":epoch}));
        }
        assert_eq!(tail.stats()["entries"], 130);
        tail.trim(100);
        tail.transition(&json!({"id":"primary-1","epoch":1}));
        assert_eq!(tail.stats()["entries"], 100);
        let rows = tail.history();
        assert_eq!(rows[0]["epoch"], 130);
        assert_eq!(rows[99]["epoch"], 31);
        assert!(tail.read(0).unwrap().get("sequence").is_none());
        assert!(tail.reports().unwrap().is_empty());
        assert_eq!(Tail::default().stats()["entries"], 0);
    }
}
