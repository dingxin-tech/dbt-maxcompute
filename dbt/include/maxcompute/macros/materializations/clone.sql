{% macro maxcompute__can_clone_table() %}
    {{ return(True) }}
{% endmacro %}


{% macro maxcompute__create_or_replace_clone(this_relation, defer_relation) %}
    clone table {{ this_relation.render() }} to {{ defer_relation.render() }} if exists overwrite;
{% endmacro %}
