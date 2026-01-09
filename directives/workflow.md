# Workflow Directives

## Byte Comparison
- **Never load raw bytes into context window for comparison**
- **Raw bytes from debug output should be written to files**
   - `cargo test --test integration_test -- --nocapture 2>&1 > debug-rs.log`
   - `PYO_DEBUG_PACKETS=1 uv run path/to/script.py > debug-py.log`
- **Create your own scripts to compare hex dumps**
   - Write small Python scripts in `directives/scripts/` for reuse. It can be structured as:
    ```python
    def load_hex_part(filename, start_line, end_line):
        # Load the corresponding lines from the file and parse then into array of hex bytes
        # Return ['1A', '2B', '3C', ...]
        # Uppercase if possible
        return [...]
    def compare_hex_arrays(arr1, arr2):
        # Compare two arrays of hex bytes and print differences
        differences = []
        # Add to differences, it should be like: {'offset': 10, 'rs_byte': '1A', 'py_byte': '2B'}
        return differences
    ```

## Context Management
- Large binary dumps = wasted tokens
- Prefer: grep for specific bytes, targeted offset reads
- When debugging packets: log hex to file, inspect with `xxd | head -50`

## Reference Code Reading
- Don't paste entire Python files into context
- Read specific functions: `sed -n '100,150p' file.py`
- Use grep to find relevant sections first

## Pyx - Cython Specifics
- Cython code can be tricky; focus on `.pyx` and `.pxd` files
- Look for `cdef` functions and `cpdef` functions for performance-critical code
- Try to create a tools to extract the Cython functions and dependency, similar to tree sitter parsing.
- Try to organize your knowledge about the Cython codebase in a separate document for future reference so you understand where to look for information.