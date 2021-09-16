
 
 /*
 select paymentmethod,
{%  for method  in paymentmethod %}
    array1.append( '{{ method }}') 

{% endfor %}
from {{ ref('test2') }}
*/

select
 {% set x = range(5) %}
{%  for i in x %}
     '{{ i }}' test
    {% if not loop.last -%}
    union all
    select
    {%- endif %}
   {% endfor %}
   








