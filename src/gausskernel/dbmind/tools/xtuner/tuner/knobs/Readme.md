# Knobs
These are tuning knob configuration files. Default using file is knobs_htap.py,
we write some demo knobs there and you could modify it before tuning.

**Note that a reboot is required for some knobs to take effect. So you have to stop your other executing jobs when tuning.**

What's the difference between reinforcement learning(rl) knobs and 
heuristic knobs?
We suggest that correlative knobs that have no relation to database internal state should be added to pso_knobs list and 
others should be added to rl_knobs list. But in fact, you can configure it according to your own scenario.

# Format
```python
{
    "shared_buffers": {  # name
        "default": 65536,  # default value for your current database, use 'select name, setting from pg_settings;' to search.
        "max": 1048576,  # upper limit. The high the value, the more difficult it is to converge.
        "min": 16,  # lower limit
        "unit": "8kB",  # unused
        "type": "int",  # {int, float, bool}
        "reboot": True  # default value is False
    }
}
```