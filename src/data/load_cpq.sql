select nuu_art as NUM_ART , max( Standard_CPQ) as CPQ
from `dfdp-datasolution-v5utdn.dev_001.standard_cpq`
group by nuu_art