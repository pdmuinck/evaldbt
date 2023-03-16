use std::collections::hash_map::Entry;
use std::fmt::Display;
use std::collections::{HashMap,HashSet};
use std::fs::{read_to_string};
use serde::{Deserialize, Serialize};
use clap::{Parser,ValueEnum};

#[derive(Parser)]
#[structopt(name="evaldbt", about="A super fast dbt project evaluator")]
struct Args {
    #[arg(short, long)]
    path: String,

    #[arg(short, long, value_enum, ignore_case = true)]
    rules: Option<Vec<NodeTest>>
}


#[derive(Serialize, Deserialize, Debug, Clone)]
struct Column {
    name: String,
    description: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    name: String,
    resource_type: String,
    fqn: Vec<String>,
    #[serde(default)]
    refs: Vec<Vec<String>>,
    #[serde(default)]
    sources: Vec<Vec<String>>,
    columns: HashMap<String, Column>,
    #[serde(default)]
    child_map: HashSet<String>,
    #[serde(default)]
    parent_map: HashSet<String>
}

#[derive(Serialize, Deserialize, Debug)]
struct Manifest {
    nodes: HashMap<String, Node>,
    parent_map: HashMap<String, Vec<String>>,
    child_map: HashMap<String, Vec<String>>
}

impl Manifest {
    pub fn from_str(data: &str) -> Self {
        let manifest: Manifest = serde_json::from_str(data).unwrap();
        let mut nodes = HashMap::new();
        for (key, mut node) in manifest.nodes {
           node.child_map = HashSet::new();
           node.parent_map = HashSet::new();

           if let Some(leafs) = manifest.child_map.get(&key) {
               for leaf in leafs {
                   node.child_map.insert(leaf.to_string());
               }
           }
           if let Some(leafs) = manifest.parent_map.get(&key) {
               for leaf in leafs {
                   node.parent_map.insert(leaf.to_string());
               }
           }
           nodes.insert(key, node);
        }
       Manifest{
           nodes,
           parent_map: manifest.parent_map,
           child_map: manifest.child_map
       }
    }
}

#[derive(ValueEnum, Clone)]
pub enum NodeTest {
    DirectJoinSource,
    MartsOrIntermediateOnSource,
    HardCodedReferences,
    ModelFanOut,
    MultipleSourcesJoined,
    NoParents,
    StagingOnDownstream,
    SourceFanOut,
    StagingOnStaging,
    UnusedSources,
    NamingConventions,
    BadDirectory
}

impl NodeTest {
    pub fn is_invalid(&self, node: &Node) -> bool {
        match self {
            NodeTest::DirectJoinSource => {
                node.resource_type == "model" && !node.sources.is_empty() && !node.refs.is_empty()
            }
            NodeTest::MartsOrIntermediateOnSource => {
                node.resource_type == "model" && !node.sources.is_empty() &&
                    (node.fqn.contains(&String::from("marts")) || node.fqn.contains(&String::from("intermediate")))
            }
            NodeTest::HardCodedReferences => {
                node.resource_type == "model" && node.refs.is_empty() && node.sources.is_empty()
            }
            NodeTest::ModelFanOut => {
                node.resource_type == "model" && node.child_map.len() > 3
            }
            NodeTest::MultipleSourcesJoined => {
                node.resource_type == "model" && node.sources.len() > 1
            }
            NodeTest::NoParents => {
                node.resource_type == "model" && node.sources.is_empty() && node.refs.is_empty()
            }
            NodeTest::StagingOnDownstream => {
                node.resource_type == "model" && node.name.starts_with("stg_") && !node.refs.is_empty()

            }
            NodeTest::SourceFanOut => {
                node.resource_type == "source" &&
                    node.child_map.iter().filter(|child| child.starts_with("model")).count() > 1
            }

            NodeTest::StagingOnStaging => {
                node.resource_type == "model" && node.name.starts_with("stg_") &&
                    node.parent_map.iter().filter(|parent| parent.starts_with("stg_")).count() > 1
            }

            NodeTest::UnusedSources => {
                // not implemented on node level
                false
            }
            NodeTest::NamingConventions => {
                (node.resource_type == "model" && node.fqn.contains(&String::from("intermediate")) && node.name.starts_with("int_")) ||
                    (node.resource_type == "model" && node.fqn.contains(&String::from("staging")) && node.name.starts_with("stg_"))
            }
            NodeTest::BadDirectory => {
                (node.resource_type == "model" && !node.fqn.contains(&String::from("staging")) && node.name.starts_with("stg_")) ||
                    (node.resource_type == "model" && !node.fqn.contains(&String::from("intermediate")) && node.name.starts_with("int_"))
            }
        }
    }
}

impl Display for NodeTest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeTest::DirectJoinSource => { write!(f, "Found models with a reference to both a model and a source") }
            NodeTest::MartsOrIntermediateOnSource => { write!(f, "Found marts or intermediates with a reference to a source") }
            NodeTest::HardCodedReferences => { write!(f, "Found models with hardcoded references") }
            NodeTest::ModelFanOut => { write!(f, "Found models with more than 3 leaf children") }
            NodeTest::MultipleSourcesJoined => { write!(f, "Found models with references to more than one source") }
            NodeTest::NoParents => { write!(f, "Found models with 0 direct parents") }
            NodeTest::StagingOnDownstream => { write!(f, "Found staging models with references to downstream models") }
            NodeTest::SourceFanOut => { write!(f, "Found sources with multiple children") }
            NodeTest::StagingOnStaging => { write!(f, "Found staging models with references to other staging models") }
            NodeTest::UnusedSources => { write!(f, "Found unused sources") }
            NodeTest::NamingConventions => { write!(f, "Found models with bad naming conventions") }
            NodeTest::BadDirectory => { write!(f, "Found models not in the appropriate directory") }
        }
    }
}


struct ValidationContext {
    node_tests: Vec<NodeTest>,
}

impl ValidationContext {
    pub fn check(&self, manifest: &Manifest) -> HashMap<String, Vec<String>> {
        let mut report = HashMap::new();
        for node in manifest.nodes.values() {
            for test in  self.node_tests.iter() {
                if test.is_invalid(node) {
                    match report.entry(test.to_string()) {
                        Entry::Vacant(e) => { e.insert(vec![node.clone().name]); }
                        Entry::Occupied(mut e) => { e.get_mut().push(node.clone().name); }
                    }
                }
            }
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_node_when_both_source_and_refs() {
        let node: Node = Node {
            name: String::from("testNode"),
            resource_type: String::from("model"),
            fqn: vec![String::from("marts")],
            refs: vec![vec![String::from("marts")]],
            sources: vec![vec![String::from("marts")]],
            columns: HashMap::new(),
            child_map: HashSet::new(),
            parent_map: HashSet::new()

        };
        assert!(NodeTest::DirectJoinSource.is_invalid(&node));
    }

    #[test]
    fn valid_node_when_only_on_sources() {
        let node: Node = Node {
            name: String::from("testNode"),
            resource_type: String::from("model"),
            fqn: vec![String::from("marts")],
            refs: vec![],
            sources: vec![vec![String::from("marts")]],
            columns: HashMap::new(),
            child_map: HashSet::new(),
            parent_map: HashSet::new()
        };

        assert!(!NodeTest::DirectJoinSource.is_invalid(&node));
    }

    #[test]
    fn invalid_node_when_marts_depends_on_source() {
        let node: Node = Node {
            name: String::from("testNode"),
            resource_type: String::from("model"),
            fqn: vec![String::from("marts")],
            refs: vec![],
            sources: vec![vec![String::from("marts")]],
            columns: HashMap::new(),
            child_map: HashSet::new(),
            parent_map: HashSet::new()
        };
        assert!(NodeTest::MartsOrIntermediateOnSource.is_invalid(&node));
    }
}

fn main() {
    let args: Args = Args::parse();

    let manifest = read_to_string(args.path).unwrap();
    let manifest: Manifest = Manifest::from_str(&manifest);

    match args.rules {
        Some(rules) => {
            let context = ValidationContext { node_tests: rules };
            let report = context.check(&manifest);
            println!("{:?}", report);
        }
        None => {
            let context = ValidationContext {
                node_tests:  vec![NodeTest::DirectJoinSource,
                    NodeTest::HardCodedReferences,
                    NodeTest::MartsOrIntermediateOnSource,
                    NodeTest::ModelFanOut,
                    NodeTest::SourceFanOut,
                    NodeTest::MultipleSourcesJoined,
                    NodeTest::NoParents,
                    NodeTest::StagingOnStaging,
                    NodeTest::StagingOnDownstream,
                    NodeTest::UnusedSources
                ]
            };

            let report = context.check(&manifest);
            println!("{:?}", report);
        }
    }
}
