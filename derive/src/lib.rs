use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse_quote;
use syn::Type;
use syn::{
    parenthesized,
    parse::{Parse, ParseBuffer, ParseStream},
    parse_macro_input, AttrStyle, Attribute, Data, DeriveInput, Expr, Ident, LitStr, Result, Token,
};

#[derive(Default)]
struct SqlArgs {
    name: Option<String>,
    foreign_key: Option<(Expr, Option<&'static str>, Option<&'static str>)>,
    unique: bool,
    conversion: Option<(Type, Expr, Expr)>,
}

impl FromIterator<SqlArgs> for SqlArgs {
    fn from_iter<T: IntoIterator<Item = SqlArgs>>(iter: T) -> Self {
        iter.into_iter()
            .reduce(|acc, next| SqlArgs {
                name: next.name.or(acc.name),
                foreign_key: next.foreign_key.or(acc.foreign_key),
                unique: acc.unique || next.unique,
                conversion: next.conversion.or(acc.conversion),
            })
            .unwrap_or_default()
    }
}

impl SqlArgs {
    fn name(&mut self, input: &ParseStream) -> Result<()> {
        input.parse::<Token![=]>()?;
        self.name = Some(input.parse::<LitStr>()?.value());
        Ok(())
    }

    fn references(&mut self, input: &ParseStream) -> Result<()> {
        let references_arg;
        parenthesized!(references_arg in input);
        let action = |input: &ParseBuffer| {
            input.parse::<Token![=]>()?;
            Ok(
                match input.parse::<LitStr>()?.value().to_lowercase().as_str() {
                    "cascade" => "cascade",
                    "restrict" => "restrict",
                    "set null" => "set null",
                    "set default" => "set default",
                    "no action" => "no action",
                    s => return Err(input.error(format!("Invalid action {}", s))),
                },
            )
        };
        let foreign_key = references_arg.parse::<Expr>()?;
        let mut on_delete = None;
        let mut on_update = None;
        loop {
            if references_arg.peek(Token![,]) {
                references_arg.parse::<Token![,]>()?;
            } else {
                break;
            }
            match references_arg.parse::<Ident>()?.to_string().as_str() {
                "on_delete" => {
                    on_delete = Some(action(&references_arg)?);
                }
                "on_update" => {
                    on_update = Some(action(&references_arg)?);
                }
                s => return Err(references_arg.error(format!("Invalid clause {}", s))),
            }
        }
        self.foreign_key = Some((foreign_key, on_update, on_delete));
        Ok(())
    }

    fn conversion(&mut self, input: &ParseStream) -> Result<()> {
        let inner;
        parenthesized!(inner in input);

        let ty = inner.parse::<Type>()?;

        let mut to_db = None;
        let mut from_db = None;

        loop {
            if inner.peek(Token![,]) {
                inner.parse::<Token![,]>()?;
            } else {
                break;
            }
            let r = match inner.parse::<Ident>()?.to_string().as_str() {
                "to" => &mut to_db,
                "from" => &mut from_db,
                n => return Err(inner.error(format!("Unexpected ident {}", n))),
            };
            *r = Some((inner.parse::<Token![=]>()?, inner.parse::<Expr>()?).1);
        }
        if let (Some(to_db), Some(from_db)) = (to_db, from_db) {
            self.conversion = Some((ty, to_db, from_db));
        } else {
            return Err(inner.error("Require both to_db and from_db"));
        };
        Ok(())
    }
}

impl Parse for SqlArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut this = Self::default();
        loop {
            let ident = input.parse::<Ident>()?;
            match ident.to_string().as_ref() {
                "name" => this.name(&input)?,
                "references" => this.references(&input)?,
                "unique" => this.unique = true,
                "db_as" => this.conversion(&input)?,
                "as_str" => {
                    this.conversion = Some((
                        parse_quote!(::std::string::String),
                        parse_quote!(|x| ::std::string::ToString::to_string(&x)),
                        parse_quote!(|x| ::core::result::Result::Ok(::core::str::FromStr::from_str(&x)?)),
                    ))
                }
                n => return Err(input.error(format!("Unexpected key {}", n))),
            }
            if !input.peek(Token![,]) {
                break;
            }
            input.parse::<Token![,]>()?;
        }
        Ok(this)
    }
}

#[proc_macro_derive(Table, attributes(sql))]
pub fn table(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens as DeriveInput);
    match table_impl(input) {
        Ok(s) => s,
        Err(e) => e.into_compile_error().into(),
    }
}

fn table_impl(input: DeriveInput) -> Result<TokenStream> {
    let data = if let Data::Struct(s) = input.data {
        s
    } else {
        return Err(syn::Error::new(
            Span::call_site(),
            "#[derive(Table)] only applies to structs",
        ));
    };
    assert!(
        input.generics.type_params().count() == 0
            && input.generics.const_params().count() == 0
            && input.generics.lifetimes().count() == 0
    );
    let sql_args = sql_attrs(&input.attrs)?;
    let name = input.ident;
    let table_name = sql_args.name.unwrap_or_else(|| name.to_string());

    let field_name = data
        .fields
        .iter()
        .map(|c| c.ident.as_ref().unwrap())
        .collect::<Vec<_>>();
    let column_type = data.fields.iter().map(|c| &c.ty).collect::<Vec<_>>();
    let db_type = data
        .fields
        .iter()
        .map(|c| {
            Ok(sql_attrs(&c.attrs)?
                .conversion
                .as_ref()
                .map(|t| &t.0)
                .unwrap_or(&c.ty)
                .clone())
        })
        .collect::<Result<Vec<_>>>()?;

    let columns = data
        .fields
        .iter()
        .map(|c| {
            let attrs = sql_attrs(&c.attrs)?;
            let quote_option = |o| {
                if let Some(s) = o {
                    quote!(::core::option::Option::Some(#s))
                } else {
                    quote!(::core::option::Option::None)
                }
            };
            let foreign_key = attrs
                .foreign_key
                .map(|(e, on_update, on_delete)| {
                    let on_update = quote_option(on_update);
                    let on_delete = quote_option(on_delete);
                    quote!(::core::option::Option::Some((&#e, #on_update, #on_delete)))
                })
                .unwrap_or(quote!(::core::option::Option::None));
            let field_name = c.ident.as_ref().unwrap();
            let column_type = &c.ty;
            let column_name = attrs.name.unwrap_or_else(|| field_name.to_string());
            let unique = quote_bool(attrs.unique);
            let (ty, conversion) = attrs
                .conversion
                .as_ref()
                .map(|(ty, to, from)| (ty, quote!((#to, #from))))
                .unwrap_or((&c.ty, quote!((|x| x, ::core::result::Result::Ok))));
            Ok(quote! {
                const #field_name: ::sql::Column<Self, #ty, #column_type> = ::sql::Column::new(
                    #column_name,
                    #foreign_key,
                    #unique,
                    #conversion
                );
            })
        })
        .collect::<Result<TokenStream2>>()?;
    Ok(quote! {
        #[allow(non_upper_case_globals)]
        #[automatically_derived]
        impl #name {
            #columns
        }
        #[automatically_derived]
        impl ::sql::Table for #name {
            const TABLE_NAME: &'static str = #table_name;
            type Columns = (#(::sql::Column<Self, #db_type, #column_type>,)*);
            const COLUMNS: Self::Columns = (#(Self::#field_name, )*);
        }

        #[automatically_derived]
        impl ::std::convert::From<(#(#column_type, )*)> for #name {
            fn from((#(#field_name,)*): (#(#column_type,)*)) -> Self {
                Self { #(#field_name, )* }
            }
        }
    }
    .into())
}

fn quote_bool(x: bool) -> TokenStream2 {
    if x {
        quote!(true)
    } else {
        quote!(false)
    }
}

fn sql_attrs<'a, T>(attributes: T) -> Result<SqlArgs>
where
    T: IntoIterator<Item = &'a Attribute>,
    T::IntoIter: DoubleEndedIterator,
{
    attributes
        .into_iter()
        .rev()
        .filter(|&a| matches!(a.style, AttrStyle::Outer) && a.path.is_ident("sql"))
        .map(|a| a.parse_args::<SqlArgs>())
        .collect()
}
