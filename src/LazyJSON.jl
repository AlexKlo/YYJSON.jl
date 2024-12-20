module LazyJSON

export LazyJSONDoc,
    LazyJSONDict,
    LazyJSONVector,
    LazyJSONError

export parse_lazy_json,
    open_lazy_json

using ..YYJSON
import ..YYJSON: read_json_doc, open_json_doc

# ___ Errors

struct LazyJSONError <: Exception
    message::String
end

Base.show(io::IO, e::LazyJSONError) = print(io, e.message)

# ___ LazyJSONDict

struct LazyJSONDict <: AbstractDict{AbstractString,Any}
    ptr::Ptr{YYJSONVal}
    iter::YYJSONObjIter

    function LazyJSONDict(ptr::Ptr{YYJSONVal})
        iter = YYJSONObjIter()
        return new(ptr, iter)
    end
end

function Base.getindex(obj::LazyJSONDict, key::AbstractString)
    value = get(obj, key, C_NULL)
    value === C_NULL && throw(KeyError(key))
    return value
end

function Base.get(obj::LazyJSONDict, key::AbstractString, default)
    value_ptr = yyjson_obj_get(obj.ptr, key)
    return value_ptr != C_NULL ? parse_json_value(value_ptr) : default
end

function Base.iterate(obj::LazyJSONDict, state = nothing)
    iter = obj.iter
    iter_ptr = Ptr{YYJSONObjIter}(pointer_from_objref(iter))
    GC.@preserve iter begin
        if state === nothing
            yyjson_obj_iter_init(obj.ptr, iter_ptr) ||
                throw(LazyJSONError("Failed to initialize iterator"))
        end
        if yyjson_obj_iter_has_next(iter_ptr)
            key_ptr = yyjson_obj_iter_next(iter_ptr)
            val_ptr = yyjson_obj_iter_get_val(key_ptr)
            return (parse_json_string(key_ptr) => parse_json_value(val_ptr)), true
        else
            return nothing
        end
    end
end

Base.length(obj::LazyJSONDict) = yyjson_obj_size(obj.ptr)

# ___ LazyJSONVector

struct LazyJSONVector <: AbstractVector{Any}
    ptr::Ptr{YYJSONVal}
end

Base.length(vec::LazyJSONVector) = yyjson_arr_size(vec.ptr)
Base.size(vec::LazyJSONVector) = (yyjson_arr_size(vec.ptr),)

function Base.getindex(vec::LazyJSONVector, index::Integer)
    value = get(vec, index, C_NULL)
    value === C_NULL && throw(BoundsError(vec, index))
    return value
end

function Base.get(vec::LazyJSONVector, index::Integer, default)
    (1 <= index <= length(vec)) || return default
    value_ptr = yyjson_arr_get(vec.ptr, index - 1)
    return value_ptr != C_NULL ? parse_json_value(value_ptr) : default
end

# ___ LazyJSONDoc

mutable struct LazyJSONDoc{T<:Union{LazyJSONDict,LazyJSONVector}}
    doc_ptr::Ptr{YYJSONDoc}
    alc_ptr::Ptr{YYJSONAlc}
    root::T
    is_open::Bool

    function LazyJSONDoc(doc_ptr::Ptr{YYJSONDoc}, alc_ptr::Ptr{YYJSONAlc}, root::T) where {T}
        doc = new{T}(doc_ptr, alc_ptr, root, true)
        finalizer(close, doc)
        return doc
    end
end

Base.length(doc::LazyJSONDoc) = length(doc.root)
Base.isopen(doc::LazyJSONDoc) = doc.is_open
Base.keys(doc::LazyJSONDoc) = keys(doc.root)
Base.values(doc::LazyJSONDoc) = values(doc.root)
Base.iterate(doc::LazyJSONDoc) = iterate(doc.root)
function Base.getindex(doc::LazyJSONDoc, key::Union{AbstractString,Integer})
    return getindex(doc.root, key)
end
function Base.get(doc::LazyJSONDoc, key::Union{AbstractString,Integer}, default)
    return get(doc.root, key, default)
end

function Base.show(io::IO, doc::LazyJSONDoc)
    len = length(doc)
    typename = doc.root isa LazyJSONDict ? "LazyJSONDict" : "LazyJSONVector"
    return print(io, "$len-element LazyJSONDoc{$typename}")
end

function Base.close(doc::LazyJSONDoc)
    doc.is_open || return
    yyjson_doc_free(doc.doc_ptr)
    yyjson_alc_dyn_free(doc.alc_ptr)
    doc.is_open = false
    return
end

# ___ Parsing Utils

function parse_json_value(ptr::Ptr{YYJSONVal})
    if yyjson_is_str(ptr)
        parse_json_string(ptr)
    elseif yyjson_is_raw(ptr)
        parse_json_string(ptr)
    elseif yyjson_is_num(ptr)
        parse_json_number(ptr)
    elseif yyjson_is_bool(ptr)
        yyjson_get_bool(ptr)
    elseif yyjson_is_obj(ptr)
        LazyJSONDict(ptr)
    elseif yyjson_is_arr(ptr)
        LazyJSONVector(ptr)
    else
        nothing
    end
end

function parse_json_string(ptr::Ptr{YYJSONVal})
    ptr_char = yyjson_get_str(ptr)
    ptr_char === C_NULL && throw(LazyJSONError("Error parsing string"))
    return unsafe_string(ptr_char)
end

function parse_json_number(ptr::Ptr{YYJSONVal})
    return yyjson_is_real(ptr) ? yyjson_get_real(ptr) : yyjson_get_int(ptr)
end

function parse_json_root(doc_ptr::Ptr{YYJSONDoc})
    root_ptr = yyjson_doc_get_root(doc_ptr)
    root_ptr === C_NULL && throw(LazyJSONError("Error parsing root"))
    return parse_json_value(root_ptr)
end

# ___ API

function parse_lazy_json(json::AbstractString; kw...)
    allocator = yyjson_alc_dyn_new()
    doc_ptr = read_json_doc(json; alc = allocator, kw...)
    root = parse_json_root(doc_ptr)
    return LazyJSONDoc(doc_ptr, allocator, root)
end

function parse_lazy_json(f::Function, json::AbstractString; kw...)
    doc = parse_lazy_json(json; kw...)
    try
        f(doc)
    finally
        close(doc)
    end
end

function open_lazy_json(path::AbstractString; kw...)
    allocator = yyjson_alc_dyn_new()
    doc_ptr = open_json_doc(path; alc = allocator, kw...)
    root = parse_json_root(doc_ptr)
    return LazyJSONDoc(doc_ptr, allocator, root)
end

function open_lazy_json(f::Function, path::AbstractString; kw...)
    doc = open_lazy_json(path; kw...)
    try
        f(doc)
    finally
        close(doc)
    end
end

end
