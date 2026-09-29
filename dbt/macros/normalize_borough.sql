{#- Map the free-text borough field onto the five boroughs.
    The feed mostly uses upper-case names, but older records and some agencies
    use county names (Kings, Richmond, New York) or mixed case. Anything else,
    including the literal "Unspecified", becomes UNSPECIFIED. -#}
{% macro normalize_borough(column) -%}
    case upper(trim({{ column }}))
        when 'MANHATTAN' then 'MANHATTAN'
        when 'NEW YORK' then 'MANHATTAN'
        when 'BROOKLYN' then 'BROOKLYN'
        when 'KINGS' then 'BROOKLYN'
        when 'QUEENS' then 'QUEENS'
        when 'BRONX' then 'BRONX'
        when 'THE BRONX' then 'BRONX'
        when 'STATEN ISLAND' then 'STATEN ISLAND'
        when 'RICHMOND' then 'STATEN ISLAND'
        else 'UNSPECIFIED'
    end
{%- endmacro %}
