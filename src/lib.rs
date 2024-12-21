#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;

use bindings::{
    exports::supabase::wrappers::routines::Guest, // Corrected import path
    supabase::wrappers::{
        http,
        types::{Cell, Context, FdwError, FdwResult, OptionsType, Row},
        utils,
    },
};

#[derive(Debug, Default)]
struct ExampleFdw {
    base_url: String,
    phone_number: String,
    from_number: String,
    api_key: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
}

// Pointer for the static FDW instance
static mut INSTANCE: *mut ExampleFdw = std::ptr::null_mut::<ExampleFdw>();

impl ExampleFdw {
    // Initialize FDW instance
    fn init_instance() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }
}

impl Guest for ExampleFdw {
    fn host_version_requirement() -> String {
        // Semver expression for Wasm FDW host version requirement
        // Ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init_instance();
        let this = Self::this_mut();

        // Retrieve API options from foreign server options
        let opts = ctx.get_options(OptionsType::Server);
        // Fetch required options without using `unwrap_or_default`
        this.phone_number = opts.require_or("phone_number", "");
        this.from_number = opts.require_or("from_number", "");
        this.api_key = opts.require_or("api_key", "");

        // Validate that all required options are provided
        if this.phone_number.is_empty() {
            return Err("Missing required option: phone_number".to_string());
        }
        if this.from_number.is_empty() {
            return Err("Missing required option: from_number".to_string());
        }
        if this.api_key.is_empty() {
            return Err("Missing required option: api_key".to_string());
        }

        // Set the base URL for WhatsApp Catalog API
        this.base_url = "https://api.p.2chat.io/open/whatsapp/catalog/products".to_string();

        Ok(())
    }

    fn begin_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // Construct the request URL with phone_number and from_number
        let url = format!(
            "{}/{}?from_number={}",
            this.base_url,
            this.phone_number,
            this.from_number
        );

        // Set up request headers
        let headers: Vec<(String, String)> = vec![
            ("user-agent".to_owned(), "WhatsApp Catalog FDW".to_owned()),
            ("X-User-API-Key".to_owned(), this.api_key.clone()),
        ];

        // Make a GET request to the WhatsApp Catalog API
        let req = http::Request {
            method: http::Method::Get,
            url,
            headers,
            body: String::default(),
        };
        let resp = http::get(&req).map_err(|e| format!("HTTP request failed: {}", e))?;
        let resp_json: JsonValue = serde_json::from_str(&resp.body)
            .map_err(|e| format!("Failed to parse JSON response: {}", e))?;

        // Check if the API request was successful
        if !resp_json
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return Err("API request was not successful".to_string());
        }

        // Extract the 'products' array from the response
        this.src_rows = resp_json
            .get("products")
            .ok_or("Cannot get 'products' from response")?
            .as_array()
            .ok_or("'products' is not an array")?
            .to_owned();

        // Log the number of products retrieved (visible in psql)
        utils::report_info(&format!(
            "Retrieved {} products from WhatsApp Catalog API",
            this.src_rows.len()
        ));

        Ok(())
    }

    fn iter_scan(_ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // If all products have been processed, end the scan
        if this.src_idx >= this.src_rows.len() {
            return Ok(None);
        }

        // Get the current product
        let src_row = &this.src_rows[this.src_idx];

        // Iterate through each target column and map source data
        for tgt_col in _ctx.get_columns() {
            // Bind the column name to ensure the String lives long enough
            let col_name = tgt_col.name();
            let tgt_col_name = col_name.as_str(); // Convert String to &str

            let cell = match tgt_col_name {
                "id" => src_row
                    .get("id")
                    .and_then(|v| v.as_str())
                    .map(|v| Cell::String(v.to_owned())),
                "retailer_id" => src_row
                    .get("retailer_id")
                    .and_then(|v| v.as_str())
                    .map(|v| Cell::String(v.to_owned())),
                "name" => src_row
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(|v| Cell::String(v.to_owned())),
                "description" => src_row
                    .get("description")
                    .and_then(|v| v.as_str())
                    .map(|v| Cell::String(v.to_owned())),
                "url" => src_row
                    .get("url")
                    .and_then(|v| v.as_str())
                    .map(|v| Cell::String(v.to_owned())),
                "currency" => src_row
                    .get("currency")
                    .and_then(|v| v.as_str())
                    .map(|v| Cell::String(v.to_owned())),
                "price" => src_row
                    .get("price")
                    .and_then(|v| v.as_str())
                    .map(|v| Cell::String(v.to_owned())),
                "is_hidden" => src_row
                    .get("is_hidden")
                    .and_then(|v| v.as_bool())
                    .map(|v| Cell::Bool(v)),
                "max_available" => src_row
                    .get("max_available")
                    .and_then(|v| v.as_i64())
                    .map(|v| Cell::I64(v)),
                "availability" => src_row
                    .get("availability")
                    .and_then(|v| v.as_str())
                    .map(|v| Cell::String(v.to_owned())),
                "checkmark" => src_row
                    .get("checkmark")
                    .and_then(|v| v.as_bool())
                    .map(|v| Cell::Bool(v)),
                "whatsapp_product_can_appeal" => src_row
                    .get("whatsapp_product_can_appeal")
                    .and_then(|v| v.as_bool())
                    .map(|v| Cell::Bool(v)),
                "is_approved" => src_row
                    .get("is_approved")
                    .and_then(|v| v.as_bool())
                    .map(|v| Cell::Bool(v)),
                "approval_status" => src_row
                    .get("approval_status")
                    .and_then(|v| v.as_str())
                    .map(|v| Cell::String(v.to_owned())),
                "signedShimmedUrl" => src_row
                    .get("signedShimmedUrl")
                    .and_then(|v| v.as_str())
                    .map(|v| Cell::String(v.to_owned())),
                "images" => {
                    // Concatenate all image URLs into a single string
                    if let Some(images) = src_row.get("images").and_then(|v| v.as_array()) {
                        let urls: Vec<String> = images
                            .iter()
                            .filter_map(|img| img.get("url").and_then(|u| u.as_str()).map(|s| s.to_owned()))
                            .collect();
                        Some(Cell::String(urls.join(", ")))
                    } else {
                        None
                    }
                }
                _ => {
                    // Unsupported column
                    return Err(format!(
                        "Column '{}' is not supported by the WhatsApp Catalog FDW",
                        tgt_col_name
                    )
                    .into());
                }
            };

            // Push the cell value to the target row
            row.push(cell.as_ref());
        }

        // Move to the next product
        this.src_idx += 1;

        // Indicate that a row has been processed
        Ok(Some(0))
    }

    fn re_scan(_ctx: &Context) -> FdwResult {
        Err("Re-scan on foreign table is not supported".to_owned())
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        this.src_idx = 0; // Reset the index
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("Modify operations on foreign table are not supported".to_owned())
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Ok(())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Ok(())
    }
}

bindings::export!(ExampleFdw with_types_in bindings);
