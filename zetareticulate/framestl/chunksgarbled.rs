use std::cmp::max;
use std::convert::TryInto;
use std::fmt::Debug;
use std::ops::Add;
use std::ops::Div;
use std::ops::Mul;
use std::ops::Sub;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::From;
use std::fmt;
use std::ops::{Deref, Index};

//Zeta Reticula Inc 2024 Apache 2.0 License; All Rights Reserved.

use ::std::rc::{
    Rc,
};

use ::std::sync::{
    Arc,
};

pub trait FromRc<T> {
    fn from_rc(val: Rc<T>) -> Self;
    fn from_arc(val: Arc<T>) -> Self;
}

// Define the ChunkedDatapoint struct to hold chunked representation of data
struct ChunkedDatapoint<T> {
    values: Vec<T>,
    cumulative_dims_per_block: Vec<u32>,
    num_blocks: usize,
}

// Define the ChunkingProjection struct
struct ChunkingProjection<T> {
    num_blocks: usize,
    dims_per_block: Vec<i32>,
    is_identity_chunk_impl: bool,
    initial_projection: Option<Box<dyn Projection<T>>>,
}

// Define the ProjectionConfig struct
struct ProjectionConfig {
    num_blocks: i32,
    variable_blocks: Vec<VariableBlock>,
    projection_type: ProjectionType,
}

// Define the VariableBlock struct
struct VariableBlock {
    num_blocks: i32,
    num_dims_per_block: i32,
}

// Define the ProjectionType enum
enum ProjectionType {
    VARIABLE_CHUNK,
    IDENTITY_CHUNK,
}

// Define the Projection trait
trait Projection<T> {
    fn project_input(&self, input: &Datapoint<T>) -> Result<ChunkedDatapoint<T>, String>;
}

// Define the Datapoint struct
struct Datapoint<T> {
    values: Vec<T>,
}

impl<T> ChunkingProjection<T> {
    // Constructor for ChunkingProjection
    fn new(num_blocks: usize, num_dims_per_block: i32) -> Self {
        let dims_per_block = vec![num_dims_per_block; num_blocks];
        Self {
            num_blocks,
            dims_per_block,
            is_identity_chunk_impl: false,
            initial_projection: None,
        }
    }

    // Constructor for identity chunking
    fn new_identity(num_blocks: usize) -> Self {
        Self {
            num_blocks,
            dims_per_block: vec![],
            is_identity_chunk_impl: true,
            initial_projection: None,
        }
    }

    // Constructor for variable block sizes
    fn new_variable(num_blocks: usize, variable_dims_per_block: Vec<i32>) -> Self {
        Self {
            num_blocks,
            dims_per_block: variable_dims_per_block,
            is_identity_chunk_impl: false,
            initial_projection: None,
        }
    }

    // Build ChunkingProjection from configuration
    fn build_from_config(config: ProjectionConfig) -> Result<Box<Self>, String> {
        match config.projection_type() {
            ProjectionType::VARIABLE_CHUNK => {
                if config.variable_blocks_size() <= 0 {
                    return Err("variable_blocks must be populated for VARIABLE_CHUNK projection".to_string());
                }
                let mut dims_per_block = Vec::new();
                let mut num_blocks = 0;
                for vblock in config.variable_blocks() {
                    if vblock.num_blocks() < 0 {
                        return Err("variable_blocks mustn't contain blocks with negative sizes".to_string());
                    }
                    dims_per_block.extend(vec![vblock.num_dims_per_block(); vblock.num_blocks() as usize]);
                    num_blocks += vblock.num_blocks();
                }
                Ok(Box::new(Self::new_variable(num_blocks, dims_per_block)))
            }
            ProjectionType::IDENTITY_CHUNK => {
                if !config.has_num_blocks() {
                    return Err("Must specify num_blocks for IDENTITY_CHUNK projection".to_string());
                }
                Ok(Box::new(Self::new_identity(config.num_blocks() as usize)))
            }
            _ => {
                // Implement other cases here

                Ok(Box::new(Self::new(0, 0)))

            }
        }
    }
}

impl ProjectionConfig {
    // Constructor for ProjectionConfig
    fn new(num_blocks: i32, variable_blocks: Vec<VariableBlock>, projection_type: ProjectionType) -> Self {
        Self {
            num_blocks,
            variable_blocks,
            projection_type,
        }
    }

    // Get the number of blocks
    fn num_blocks(&self) -> i32 {
        self.num_blocks
    }

    // Get the variable blocks
    fn variable_blocks(&self) -> &Vec<VariableBlock> {
        &self.variable_blocks
    }

    // Get the projection type
    fn projection_type(&self) -> &ProjectionType {
        &self.projection_type
    }

    // Check if the number of blocks is specified
    fn has_num_blocks(&self) -> bool {
        self.num_blocks > 0
    }

    // Get the number of variable blocks
    fn variable_blocks_size(&self) -> usize {
        self.variable_blocks.len()
    }
}

impl VariableBlock {
    // Constructor for VariableBlock
    fn new(num_blocks: i32, num_dims_per_block: i32) -> Self {
        Self {
            num_blocks,
            num_dims_per_block,
        }
    }

    // Get the number of blocks
    fn num_blocks(&self) -> i32 {
        self.num_blocks
    }

    // Get the number of dimensions per block
    fn num_dims_per_block(&self) -> i32 {
        self.num_dims_per_block
    }
}




// Define the ChunkedDatapoint struct to hold chunked representation of data
struct ChunkedDatapoint<T> {
    values: Vec<T>,
    cumulative_dims_per_block: Vec<u32>,
    num_blocks: usize,
}

// Define the ChunkingProjection struct
struct ChunkingProjection<T> {
    num_blocks: usize,
    dims_per_block: Vec<i32>,
    is_identity_chunk_impl: bool,
    initial_projection: Option<Box<dyn Projection<T>>>,
}

// Define the Projection trait
trait Projection<T> {
    fn project_input(&self, input: &Datapoint<T>) -> Result<ChunkedDatapoint<T>, String>;
}

// Define the Datapoint struct
struct Datapoint<T> {
    values: Vec<T>,
}

impl<T> ChunkingProjection<T> {
    // Constructor for ChunkingProjection
    fn new(num_blocks: usize, num_dims_per_block: i32) -> Self {
        let dims_per_block = vec![num_dims_per_block; num_blocks];
        Self {
            num_blocks,
            dims_per_block,
            is_identity_chunk_impl: false,
            initial_projection: None,
        }
    }



    // Constructor for identity chunking
    fn new_identity(num_blocks: usize) -> Self {
        Self {
            num_blocks,
            dims_per_block: vec![],
            is_identity_chunk_impl: true,
            initial_projection: None,
        }
    }

    // Constructor for variable block sizes
    fn new_variable(num_blocks: usize, variable_dims_per_block: Vec<i32>) -> Self {
        Self {
            num_blocks,
            dims_per_block: variable_dims_per_block,
            is_identity_chunk_impl: false,
            initial_projection: None,
        }
    }

    // Build ChunkingProjection from configuration
    fn build_from_config(config: ProjectionConfig) -> Result<Box<Self>, String> {
        match config.projection_type() {
            ProjectionType::VARIABLE_CHUNK => {
                if config.variable_blocks_size() <= 0 {
                    return Err("variable_blocks must be populated for VARIABLE_CHUNK projection".to_string());
                }
                let mut dims_per_block = Vec::new();
                let mut num_blocks = 0;
                for vblock in config.variable_blocks() {
                    if vblock.num_blocks() < 0 {
                        return Err("variable_blocks mustn't contain blocks with negative sizes".to_string());
                    }
                    dims_per_block.extend(vec![vblock.num_dims_per_block(); vblock.num_blocks() as usize]);
                    num_blocks += vblock.num_blocks();
                }
                Ok(Box::new(Self::new_variable(num_blocks, dims_per_block)))
            }
            ProjectionType::IDENTITY_CHUNK => {
                if !config.has_num_blocks() {
                    return Err("Must specify num_blocks for IDENTITY_CHUNK projection".to_string());
                }
                Ok(Box::new(Self::new_identity(config.num_blocks() as usize)))
            }
            _ => {
                // Implement other cases here

                Ok(Box::new(Self::new(0, 0)))

            }
        }
    }
}

impl<T> Projection<T> for ChunkingProjection<T> {
    // Implement Projection trait for ChunkingProjection
    fn project_input(&self, input: &Datapoint<T>) -> Result<ChunkedDatapoint<T>, String> {
        if self.is_identity_chunk_impl {
            // Implement identity chunking
            let mut cumulative_dims_per_block = vec![0; self.num_blocks];
            let mut values = Vec::new();
            for i in 0..self.num_blocks {
                let start = cumulative_dims_per_block[i] as usize;
                let end = start + self.dims_per_block[i] as usize;
                values.extend(input.values[start..end].iter().cloned());
                cumulative_dims_per_block[i] += self.dims_per_block[i] as u32;
            }
            Ok(ChunkedDatapoint {
                values,
                cumulative_dims_per_block,
                num_blocks: self.num_blocks,
            })
        } else {
            // Implement variable chunking
            let mut cumulative_dims_per_block = vec![0; self.num_blocks];
            let mut values = Vec::new();
            for i in 0..self.num_blocks {
                let start = cumulative_dims_per_block[i] as usize;
                let end = start + self.dims_per_block[i] as usize;
                values.extend(input.values[start..end].iter().cloned());
                cumulative_dims_per_block[i] += self.dims_per_block[i] as u32;
            }
            Ok(ChunkedDatapoint {
                values,
                cumulative_dims_per_block,
                num_blocks: self.num_blocks,
            })
        }
    }
    }

// Define the ProjectionConfig struct
struct ProjectionConfig {
    num_blocks: i32,
    variable_blocks: Vec<VariableBlock>,
    projection_type: ProjectionType,
}

// Define the VariableBlock struct
struct VariableBlock {
    num_blocks: i32,
    num_dims_per_block: i32,
}

// Define the ProjectionType enum
enum ProjectionType {
    VARIABLE_CHUNK,
    IDENTITY_CHUNK,
}

impl ProjectionConfig {
    // Constructor for ProjectionConfig
    fn new(num_blocks: i32, variable_blocks: Vec<VariableBlock>, projection_type: ProjectionType) -> Self {
        Self {
            num_blocks,
            variable_blocks,
            projection_type,
        }
    }

    // Get the number of blocks
    fn num_blocks(&self) -> i32 {
        self.num_blocks
    }

    // Get the variable blocks
    fn variable_blocks(&self) -> &Vec<VariableBlock> {
        &self.variable_blocks
    }

    // Get the projection type
    fn projection_type(&self) -> &ProjectionType {
        &self.projection_type
    }

    // Check if the number of blocks is specified
    fn has_num_blocks(&self) -> bool {
        self.num_blocks > 0
    }

    // Get the number of variable blocks
    fn variable_blocks_size(&self) -> usize {
        self.variable_blocks.len()
    }
}


impl VariableBlock {
    // Constructor for VariableBlock
    fn new(num_blocks: i32, num_dims_per_block: i32) -> Self {
        Self {
            num_blocks,
            num_dims_per_block,
        }
    }

    // Get the number of blocks
    fn num_blocks(&self) -> i32 {
        self.num_blocks
    }

    // Get the number of dimensions per block
    fn num_dims_per_block(&self) -> i32 {
        self.num_dims_per_block
    }
}


// Define the ChunkedDatapoint struct to hold chunked representation of data
struct ChunkedDatapoint<T> {
    values: Vec<T>,
    cumulative_dims_per_block: Vec<u32>,
    num_blocks: usize,
}

// Define the ChunkingProjection struct
struct ChunkingProjection<T> {
    num_blocks: usize,
    dims_per_block: Vec<i32>,
    is_identity_chunk_impl: bool,
    initial_projection: Option<Box<dyn Projection<T>>>,
}
