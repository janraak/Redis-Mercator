use regex::Regex;
// use std::str::pattern;
use uuid::Uuid;

#[macro_use]
extern crate redis_module;

use redis_module::{/*parse_integer, */ Context, RedisError, RedisResult, RedisValue, REDIS_OK,};

const VALUE_ONLY: usize = 2;
const FIELD_AND_VALUE_ONLY: usize = 3;
const FIELD_OP_VALUE: usize = 4;

struct MatchSet {
    set_id: String,
    tally: i64,
}

// fn print_type_of<T>(_: &T) {
//     println!("{}", std::any::type_name::<T>())
// }

fn match_keys(ctx: &Context, pattern: String) -> MatchSet {
    let my_uuid = Uuid::new_v4();
    let my_uuid_str: String = my_uuid.to_hyphenated().to_string();
    let mut my_tally: i64 = 0;
    let r = ctx.call("KEYS", &[&pattern]);
    match r {
        Ok(data) => match data {
            RedisValue::Array(ref keys) => {
                for key in keys.iter() {
                    match key {
                        RedisValue::SimpleString(s) => {
                            let result = ctx.call(
                                "ZUNIONSTORE",
                                &[&my_uuid_str, "2", &my_uuid_str, &s, "AGGREGATE", "SUM"],
                            );
                            match result {
                                Ok(RedisValue::Integer(tally)) => {
                                    my_tally = tally;
                                }
                                _ => {}
                            };
                        }
                        _ => {}
                    }
                }
                return MatchSet {
                    set_id: my_uuid_str,
                    tally: my_tally,
                };
            }
            _ => {}
        },
        _ => {}
    }
    return MatchSet {
        set_id: my_uuid_str,
        tally: 0,
    };
}

fn compare_strings(left: String, operator: String, right: String) -> bool {
    if operator == "==" {
        return left == right;
    }
    if operator == ">=" {
        return left >= right;
    }
    if operator == "<=" {
        return left <= right;
    }
    if operator == "!=" {
        return left != right;
    }
    if operator == "<>" {
        return left != right;
    }
    if operator == "<" {
        return left < right;
    }
    if operator == ">" {
        return left > right;
    }
    return false;
}

fn compare_numbers(left: f64, operator: String, right: f64) -> bool {
    if operator == "==" {
        return left == right;
    }
    if operator == ">=" {
        return left >= right;
    }
    if operator == "<=" {
        return left <= right;
    }
    if operator == "!=" {
        return left != right;
    }
    if operator == "<>" {
        return left != right;
    }
    if operator == "<" {
        return left < right;
    }
    if operator == ">" {
        return left > right;
    }
    return false;
}

fn match_keys_conditionally(
    ctx: &Context,
    pattern: String,
    operator: String,
    value: String,
) -> MatchSet {
    let my_uuid = Uuid::new_v4();
    let my_uuid_str: String = my_uuid.to_hyphenated().to_string();
    let mut my_tally: i64 = 0;
    // print_type_of(&my_uuid);
    // print_type_of(&my_uuid_str);
    let r = ctx.call("KEYS", &[&pattern]);
    let fvalue_try_parse = value.parse::<f64>();
    let mut numeric_comparison = false;
    let mut float_compare_value = 0.0;
    match fvalue_try_parse {
        Ok(f) => {
            numeric_comparison = true;
            float_compare_value = f;
        }
        _ => {}
    }
    match r {
        Ok(data) => match data {
            RedisValue::Array(ref keys) => {
                let re = Regex::new(r"(?:_zx_[:])(\w+)(?:[:])(\w+)").unwrap();
                for key in keys.iter() {
                    // Key format _zx_:<value>:<field>
                    match key {
                        RedisValue::SimpleString(s) => {
                            for cap in re.captures_iter(s) {
                                if numeric_comparison {
                                    let fvalue_try_parse = cap[1].parse::<f64>();
                                    match fvalue_try_parse {
                                        Ok(f) => {
                                            if compare_numbers(
                                                float_compare_value,
                                                operator.to_string(),
                                                f,
                                            ) {
                                                continue;
                                            }
                                        }
                                        Err(_) => continue,
                                    }
                                } else {
                                    if compare_strings(
                                        value.to_string(),
                                        operator.to_string(),
                                        s.to_string(),
                                    ) {
                                        continue;
                                    }
                                }

                                let result = ctx.call(
                                    "ZUNIONSTORE",
                                    &[&my_uuid_str, "2", &my_uuid_str, &s, "AGGREGATE", "SUM"],
                                );
                                match result {
                                    Ok(RedisValue::Integer(tally)) => {
                                        my_tally = tally;
                                    }
                                    _ => {}
                                };
                            }
                        }
                        _ => {}
                    }
                }
                return MatchSet {
                    set_id: my_uuid_str,
                    tally: my_tally,
                };
            }
            _ => {}
        },
        _ => {}
    }
    return MatchSet {
        set_id: my_uuid_str,
        tally: 0,
    };
}

fn rx_add(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() < 6 {
        return Err(RedisError::WrongArity);
    }

    let object_key = &args[1];
    let key_type = &args[2];
    let field_name = &args[3];
    let token_value = &args[4];
    let confidence = &args[5];
    // let db_id = &args[6];

    // if !db_id.trim().is_empty() {
    //     let _result = ctx.call("SELECT", &[&db_id]);
    // }

    let object_reference = format!("{}\t{}", object_key, key_type);
    let value_indexkey = format!("_zx_:{}:{}", token_value, field_name);

    // let fieldname_indexkey = format!("_zx_:{}:_kw_", field_name);

    let _result = ctx.call("ZADD", &[&value_indexkey, &confidence, &object_reference]);
    let rev_object_key = format!("_ox_:{}", object_key);
    let _result = ctx.call("SADD", &[&rev_object_key, &value_indexkey]);

    return REDIS_OK; //Ok(RedisValue::SimpleStringStatic("OK"));
}

/*
 * Fetch member set for query sub-expression
 */
fn rx_fetch(ctx: &Context, args: Vec<String>) -> RedisResult {
    // let _dbId = raw::RedisModule_GetSelectedDb(ctx);

    // int dbId = RedisModule_GetSelectedDb(ctx);
    // redisDb *db = server.db + dbId;
    // list *matchingKeys;

    let data: MatchSet = match args.len() {
        VALUE_ONLY => {
            let pattern = format!("*:{}:*", &args[1]);
            let k = match_keys(ctx, pattern);
            k
        }
        FIELD_AND_VALUE_ONLY => {
            let pattern = format!("*{}*:{}", &args[1], &args[2]);
            let k = match_keys(ctx, pattern);
            k
        }
        FIELD_OP_VALUE => {
            let value = &args[1];
            let op = &args[3];
            let pattern = format!("*:*:{}", &args[2]);
            let k = match_keys_conditionally(ctx, pattern, op.to_string(), value.to_string());
            k
        }
        _ => {
            return Err(RedisError::WrongArity);
        }
    };
    let row_count = format!("{}", data.tally);
    let result = ctx.call("ZREVRANGE", &[&data.set_id, "0", &row_count, "WITHSCORES"]);
    ctx.reply(result);
    return Ok(RedisValue::NoReply); // Ok already part of result!
}

fn rx_describe(ctx: &Context, args: Vec<String>) -> RedisResult {
    // Describe what we know about a 'key'
    //
    // Eg.
    // rxadd jan "s" spouse isabel 1.0 2
    // rxadd wim "s" daughter isabel 1.0 2
    // rxadd bep "s" daughter isabel 1.0 2
    //
    // rxDescribe isa
    //
    // will return
    //
    // spouse
    // daughter
    //
    let pattern = format!("_zx_:{}:*", &args[1]);
    let result = ctx.call("KEYS", &[&pattern]);
    ctx.reply(result);
    return Ok(RedisValue::NoReply); // Ok already part of result!
}

fn rx_begin_key(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }

    let object_key = &args[1];
    let rev_object_key = format!("_ox_:{}", object_key);
    let rev_object_key_transaction = format!("#ox_:{}", object_key);
    // let _result = ctx.call("SUNIONSTORE", &[&rev_object_key_transaction, &rev_object_key]);
    let _result = ctx.call("RENAME", &[&rev_object_key, &rev_object_key_transaction]);

    return REDIS_OK; //Ok(RedisValue::SimpleStringStatic("OK"));
}

fn rx_commit_key(ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }

    let object_key = &args[1];
    let rev_object_key = format!("_ox_:{}", object_key);
    let rev_object_key_transaction = format!("#ox_:{}", object_key);
    let member_diff = ctx
        .call("SDIFF", &[&rev_object_key_transaction, &rev_object_key])
        .unwrap();
    match member_diff {
        RedisValue::Array(members) => {
            for diff_member in &members {
                match diff_member {
                    RedisValue::SimpleString(member_from_diff) => {
                        let object_key_for_h = format!("{}\tH", object_key);
                        let object_key_for_s = format!("{}\tS", object_key);
                        ctx.call("ZREM", &[&member_from_diff, &object_key_for_h])
                            .unwrap();
                        ctx.call("ZREM", &[&member_from_diff, &object_key_for_s])
                            .unwrap();
                    }
                    _ => {
                        return Err(RedisError::String("Unexpected key types".to_string()));
                    }
                }
            }
        }
        _ => {
            return Err(RedisError::String("Unexpected key types".to_string()));
        }
    }
    ctx.call("DEL", &[&rev_object_key_transaction]).unwrap();
    return REDIS_OK; //Ok(RedisValue::SimpleStringStatic("OK"));
}

fn rx_rollback_key(_ctx: &Context, args: Vec<String>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }

    // let object_key = &args[1];
    // let rev_object_key = format!("_ox_:{}", object_key);
    // let rev_object_key_transaction = format!("#ox_:{}", object_key);

    return REDIS_OK; //Ok(RedisValue::SimpleStringStatic("OK"));
}

//////////////////////////////////////////////////////

redis_module! {
    name: "rx-fetch",
    version: 1,
    data_types: [],
    commands: [
        ["rxAdd", rx_add, "", 0, 0, 0],
        ["rxBegin", rx_begin_key, "", 0, 0, 0],
        ["rxCommit", rx_commit_key, "", 0, 0, 0],
        ["rxRollback", rx_rollback_key, "", 0, 0, 0],
        ["rxFetch", rx_fetch, "", 0, 0, 0],
        ["rxDescribe", rx_describe, "", 0, 0, 0],
    ],
}

//////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use redis_module::RedisValue;

    fn run_rx_add(args: &[&str]) -> RedisResult {
        rx_add(
            &Context::dummy(),
            args.iter().map(|v| String::from(*v)).collect(),
        )
    }

    #[test]
    fn rx_add_valid_integer_args() {
        let result = run_rx_add(&vec!["amazon", "s", "org", "aws", "1.0", "2"]);

        match result {
            Ok(RedisValue::String(v)) => {
                assert_eq!(v, "OK");
            }
            _ => assert!(false, "Bad result: {:?}", result),
        }
    }

    #[test]
    fn rx_add_bad_integer_args() {
        let result = run_rx_add(&vec!["amazon", "s", "org", "aws", "1.0", "2"]);

        match result {
            Err(RedisError::String(s)) => {
                assert_eq!(s, "Wrong arity");
            }
            _ => assert!(false, "Bad result: {:?}", result),
        }
    }
}
