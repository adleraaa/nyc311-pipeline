{#- The feed's created/closed dates are New York wall-clock time without an
    offset. timezone('America/New_York', ts) reads a naive timestamp as NY local
    time (DuckDB's bundled ICU knows the DST rules); timezone('UTC', ...) turns
    the result back into a naive UTC timestamp. Durations computed on the UTC
    values are correct across DST changes, which naive local arithmetic is not
    (2026-11-01 00:30 -> 03:30 local is 4 real hours, not 3). -#}
{% macro ny_to_utc(column) -%}
    timezone('UTC', timezone('America/New_York', {{ column }}))
{%- endmacro %}
