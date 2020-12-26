#[macro_use]
extern crate quote;
#[macro_use]
extern crate syn;

use syn::DeriveInput;

use proc_macro::TokenStream;

#[proc_macro_derive(Function)]
pub fn derive_function(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Build the output, possibly using quasi-quotation
    let name = &input.ident;
    let (im, ty, wh) = input.generics.split_for_impl();
    let expanded = quote! {
        impl #im rlink::api::function::Function for #name #ty #wh {
            fn get_name(&self) -> &str {
                stringify!(#name)
            }
        }
    };

    // Hand the output tokens back to the compiler
    TokenStream::from(expanded)
}

#[proc_macro_attribute]
#[cfg(not(test))]
pub fn main(args: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = syn::parse_macro_input!(item as syn::ItemFn);
    let args = syn::parse_macro_input!(args as syn::AttributeArgs);

    let sig = &mut input.sig; // eg: struct Name, function sig
    let name = &sig.ident;
    let inputs = &sig.inputs;
    let body = &input.block;
    let _attrs = &input.attrs;
    let _vis = input.vis; // eg: pub, pub(crate), etc...

    if sig.asyncness.is_some() {
        let msg = "the async keyword is unsupported from the function declaration";
        return syn::Error::new_spanned(sig.fn_token, msg)
            .to_compile_error()
            .into();
    } else if name != "main" {
        let msg = "the function name must be `main`";
        return syn::Error::new_spanned(&sig.inputs, msg)
            .to_compile_error()
            .into();
    } else if inputs.is_empty() {
        let msg = "the main function accept arguments";
        return syn::Error::new_spanned(&sig.inputs, msg)
            .to_compile_error()
            .into();
    }

    // if args.len() != 1 {
    //     let msg = "the main function accept arguments";
    //     return syn::Error::new_spanned(&args., msg)
    //         .to_compile_error()
    //         .into();
    // }

    let arg0 = &args[0];
    let stream_fn = if let syn::NestedMeta::Meta(syn::Meta::Path(path)) = arg0 {
        let ident = path.get_ident();
        if ident.is_none() {
            let msg = "Must have specified ident";
            return syn::Error::new_spanned(path, msg).to_compile_error().into();
        }

        ident.unwrap()
    } else {
        let msg = "Must have specified ident..";
        return syn::Error::new_spanned(arg0, msg).to_compile_error().into();
    };

    let result = quote! {
        #[derive(Clone, Debug)]
        pub struct GenStreamJob {}

        impl rlink::api::env::StreamJob for GenStreamJob {
            fn prepare_properties(&self, properties: &mut Properties) {
                #body
            }

            fn build_stream(
                &self,
                properties: &Properties,
                env: &StreamExecutionEnvironment,
            ) -> SinkStream {
                #stream_fn(properties, env)
            }
        }

        fn main() {
            rlink::api::env::execute("job_name", GenStreamJob{});
        }
    };

    // println!("{}", result.to_string());
    result.into()
}
