use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, Ident, ItemImpl, PatType, ReturnType, Type, TypeReference, parse_macro_input};

fn uppercase_first(input: &str) -> String {
    if input.is_empty() {
        "".to_string()
    } else {
        input
            .chars()
            .next()
            .unwrap()
            .to_uppercase()
            .collect::<String>()
            + &input.to_string()[1..]
    }
}

#[proc_macro_attribute]
pub fn derive_bjw_db(args: TokenStream, item: TokenStream) -> TokenStream {
    let thread_safe = match syn::parse::<Ident>(args) {
        Ok(i) => i == "thread_safe",
        Err(_) => false,
    };

    let input = parse_macro_input!(item as ItemImpl);
    let cloned = input.clone();

    let struct_name = if let Type::Path(tp) = *input.self_ty {
        tp.path.segments.last().unwrap().ident.clone()
    } else {
        panic!("Expected a struct implementation");
    };

    // some things differ between the thread_safe and the not thread_safe version
    let (wrapped_type, constructor, read_acces, write_access, mut_self, into_inner) = if thread_safe
    {
        (
            quote! { std::sync::RwLock<Database<#struct_name>> },
            quote! { Ok(Self { db: std::sync::RwLock::new(db) }) },
            quote! { self.db.read().unwrap() },
            quote! { self.db.write().unwrap() },
            quote! { &self },
            quote! { self.db.into_inner().unwrap() },
        )
    } else {
        (
            quote! { Database<#struct_name> },
            quote! { Ok(Self { db }) },
            quote! { self.db },
            quote! { self.db },
            quote! { &mut self },
            quote! { self.db },
        )
    };

    // build the names for the three enums we need
    let read_params_ident = format_ident!("{}ReadParams", struct_name);
    let read_return_ident = format_ident!("{}ReadReturn", struct_name);
    let update_params_ident = format_ident!("{}UpdateParams", struct_name);

    // build the name for the DB wrapper
    let db_struct_ident = format_ident!("{}Db", struct_name);

    // vectors for collecting relevant infos while parsing all functions
    let mut read_params_variants = Vec::new();
    let mut read_return_variants = Vec::new();
    let mut read_match_arms = Vec::new();
    let mut update_params_variants = Vec::new();
    let mut update_match_arms = Vec::new();
    let mut read_methods = Vec::new();
    let mut update_methods = Vec::new();

    // parse all function signatures
    for item in input.items.iter() {
        if let syn::ImplItem::Fn(method) = item {
            let method_name = &method.sig.ident;
            let variant_name =
                format_ident!("{}", uppercase_first(method_name.to_string().as_ref()));

            let is_read = method.sig.inputs.iter().next().is_some_and(|arg| matches!(arg, FnArg::Receiver(r) if r.reference.is_some() && r.mutability.is_none()));
            let is_update = method.sig.inputs.iter().next().is_some_and(|arg| matches!(arg, FnArg::Receiver(r) if r.reference.is_some() && r.mutability.is_some()));
            if !is_read && !is_update {
                panic!("Found strange function without a &[mut] self");
            }

            // parse arguments
            let mut arg_types = Vec::new();
            let mut arg_types_with_lifetime = Vec::new();
            let mut arg_names = Vec::new();
            for arg in method.sig.inputs.iter().skip(1) {
                if let FnArg::Typed(PatType { ty, .. }) = arg {
                    arg_types.push(quote! { #ty });
                    if let Type::Reference(TypeReference { elem, .. }) = &**ty {
                        arg_types_with_lifetime.push(quote! { &'a #elem });
                    } else {
                        arg_types_with_lifetime.push(quote! { #ty });
                    }
                    if let FnArg::Typed(PatType { pat, .. }) = arg {
                        arg_names.push(quote! { #pat });
                    } else {
                        panic!("Found strange function argument without a name: {:?}", arg);
                    }
                } else {
                    panic!("Found strange function argument without a type: {:?}", arg);
                }
            }
            let cloned_args: Vec<_> = arg_names.iter().map(|n| quote! { #n.clone()}).collect();

            let return_type = match &method.sig.output {
                ReturnType::Type(_, ty) => quote! { #ty },
                _ => quote! { () },
            };

            if is_read {
                read_params_variants.push(quote! { #variant_name(#(#arg_types_with_lifetime),*) });
                let return_type = match &method.sig.output {
                    ReturnType::Type(_, ty) => quote! { #ty },
                    _ => quote! { () },
                };
                read_return_variants.push(quote! { #variant_name(#return_type) });
                read_match_arms.push(quote! {
                    #read_params_ident::#variant_name(#(#arg_names),*) => #read_return_ident::#variant_name(self.#method_name(#(#cloned_args),*))
                });

                read_methods.push(quote! {
                    #[allow(dead_code)]
                    pub fn #method_name(&self, #(#arg_names: #arg_types),*) -> #return_type {
                        match #read_acces.read(&#read_params_ident::#variant_name(#(#arg_names),*)) {
                            #read_return_ident::#variant_name(value) => value,
                            _ => unreachable!()
                        }
                    }
                });
            } else if is_update {
                update_params_variants.push(quote! { #variant_name(#(#arg_types),*) });
                update_match_arms.push(quote! {
                    #update_params_ident::#variant_name(#(#arg_names),*) => self.#method_name(#(#cloned_args),*)
                });

                update_methods.push(quote! {
                    pub fn #method_name(#mut_self, #(#arg_names: #arg_types),*) -> Result<#return_type> {
                        #write_access.update(&#update_params_ident::#variant_name(#(#arg_names),*))
                    }
                });
            }
        }
    }

    let original = quote! { #cloned };
    let derived = quote! {
        use bjw_db::{Database, Readable, Updateable};
        type Result<T> = std::io::Result<T>;

        enum #read_params_ident<'a> {
            #(#read_params_variants),*
        }

        #[derive(Clone)]
        enum #read_return_ident {
            #(#read_return_variants),*
        }

        impl Readable for #struct_name {
            type Args<'a> = #read_params_ident<'a>;
            type ReturnType = #read_return_ident;

            fn read(&self, params: &#read_params_ident<'_>) -> Self::ReturnType {
                match params {
                    #(#read_match_arms),*
                }
            }
        }

        #[derive(serde::Serialize, serde::Deserialize)]
        enum #update_params_ident {
            #(#update_params_variants),*
        }

        impl Updateable for #struct_name {
            type Args = #update_params_ident;

            fn update(&mut self, params: &Self::Args) {
                match params {
                    #(#update_match_arms),*
                }
            }
        }

        pub struct #db_struct_ident {
            db: #wrapped_type
        }

        impl #db_struct_ident {
            pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
                let db = Database::open(path)?;
                #constructor
            }

            #(#read_methods)*
            #(#update_methods)*

            pub fn create_checkpoint(#mut_self) -> Result<()> {
                #write_access.create_checkpoint()
            }

            pub fn clone_data(&self) -> #struct_name {
                #read_acces.clone_data()
            }

            pub fn delete(self) -> Result<()> {
                #into_inner.delete()
            }
        }
    };

    quote! {
        #original
        #derived
    }
    .into()
}
