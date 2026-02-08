use std::path::Path;

use jni::objects::{JClass, JObjectArray, JString};
use jni::sys::jstring;
use jni::JNIEnv;
use reqwest::multipart::{Form, Part};

fn runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Runtime::new().map_err(|err| err.to_string())
}

fn to_rust_string(env: &mut JNIEnv<'_>, input: JString<'_>) -> Result<String, String> {
    env.get_string(&input)
        .map(|value| value.into())
        .map_err(|err| err.to_string())
}

fn to_string_vec(
    env: &mut JNIEnv<'_>,
    input: JObjectArray<'_>,
) -> Result<Vec<String>, String> {
    let len = env
        .get_array_length(&input)
        .map_err(|err| err.to_string())?;
    let mut values = Vec::with_capacity(len as usize);
    for index in 0..len {
        let element = env
            .get_object_array_element(&input, index)
            .map_err(|err| err.to_string())?;
        let element = JString::from(element);
        values.push(to_rust_string(env, element)?);
    }
    Ok(values)
}

fn throw(env: &mut JNIEnv<'_>, message: String) -> jstring {
    let _ = env.throw_new("java/lang/RuntimeException", message);
    std::ptr::null_mut()
}

fn response_to_jstring(env: &mut JNIEnv<'_>, response: String) -> jstring {
    match env.new_string(response) {
        Ok(output) => output.into_raw(),
        Err(err) => throw(env, err.to_string()),
    }
}

async fn send_get(url: &str) -> Result<String, String> {
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|err| err.to_string())?;
    response.text().await.map_err(|err| err.to_string())
}

async fn send_with_body(
    method: reqwest::Method,
    url: &str,
    body: &str,
    content_type: &str,
) -> Result<String, String> {
    let client = reqwest::Client::new();
    let response = client
        .request(method, url)
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(body.to_string())
        .send()
        .await
        .map_err(|err| err.to_string())?;
    response.text().await.map_err(|err| err.to_string())
}

async fn send_multipart(
    url: &str,
    field_names: &[String],
    field_values: &[String],
    file_field_names: &[String],
    file_paths: &[String],
) -> Result<String, String> {
    if field_names.len() != field_values.len() {
        return Err("field names and values must be same length".to_string());
    }
    if file_field_names.len() != file_paths.len() {
        return Err("file field names and file paths must be same length".to_string());
    }

    let mut form = Form::new();
    for (name, value) in field_names.iter().zip(field_values.iter()) {
        form = form.text(name.to_string(), value.to_string());
    }
    for (name, path) in file_field_names.iter().zip(file_paths.iter()) {
        let file_path = Path::new(path);
        let part = Part::file(file_path)
            .map_err(|err| format!("file {}: {err}", file_path.display()))?;
        form = form.part(name.to_string(), part);
    }

    let client = reqwest::Client::new();
    let response = client
        .post(url)
        .multipart(form)
        .send()
        .await
        .map_err(|err| err.to_string())?;
    response.text().await.map_err(|err| err.to_string())
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_com_rustwebsvc_reqwest_ReqwestClient_getNative(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    url: JString<'_>,
) -> jstring {
    let url = match to_rust_string(&mut env, url) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let runtime = match runtime() {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let response = match runtime.block_on(send_get(&url)) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    response_to_jstring(&mut env, response)
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_com_rustwebsvc_reqwest_ReqwestClient_postNative(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    url: JString<'_>,
    body: JString<'_>,
    content_type: JString<'_>,
) -> jstring {
    let url = match to_rust_string(&mut env, url) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let body = match to_rust_string(&mut env, body) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let content_type = match to_rust_string(&mut env, content_type) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let runtime = match runtime() {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let response = match runtime.block_on(send_with_body(
        reqwest::Method::POST,
        &url,
        &body,
        &content_type,
    )) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    response_to_jstring(&mut env, response)
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_com_rustwebsvc_reqwest_ReqwestClient_putNative(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    url: JString<'_>,
    body: JString<'_>,
    content_type: JString<'_>,
) -> jstring {
    let url = match to_rust_string(&mut env, url) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let body = match to_rust_string(&mut env, body) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let content_type = match to_rust_string(&mut env, content_type) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let runtime = match runtime() {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let response = match runtime.block_on(send_with_body(
        reqwest::Method::PUT,
        &url,
        &body,
        &content_type,
    )) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    response_to_jstring(&mut env, response)
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_com_rustwebsvc_reqwest_ReqwestClient_multipartNative(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    url: JString<'_>,
    field_names: JObjectArray<'_>,
    field_values: JObjectArray<'_>,
    file_field_names: JObjectArray<'_>,
    file_paths: JObjectArray<'_>,
) -> jstring {
    let url = match to_rust_string(&mut env, url) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let field_names = match to_string_vec(&mut env, field_names) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let field_values = match to_string_vec(&mut env, field_values) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let file_field_names = match to_string_vec(&mut env, file_field_names) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let file_paths = match to_string_vec(&mut env, file_paths) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };

    let runtime = match runtime() {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    let response = match runtime.block_on(send_multipart(
        &url,
        &field_names,
        &field_values,
        &file_field_names,
        &file_paths,
    )) {
        Ok(value) => value,
        Err(err) => return throw(&mut env, err),
    };
    response_to_jstring(&mut env, response)
}
