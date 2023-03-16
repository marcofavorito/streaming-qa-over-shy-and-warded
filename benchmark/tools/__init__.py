from benchmark.tools.core import ToolID, ToolRegistry
from benchmark.tools.dlve import DLVE_WRAPPER_PATH, DlvTool
from benchmark.tools.vadalog import VADALOG_WRAPPER_PATH, VadalogTool

tool_registry = ToolRegistry()

tool_registry.register(
    ToolID.VADALOG,
    item_cls=VadalogTool,
    binary_path=VADALOG_WRAPPER_PATH,
    properties=dict(terminationStrategyMode="lightMode"),
)
tool_registry.register(
    ToolID.VADALOG_PARSIMONIOUS_NAIVE,
    item_cls=VadalogTool,
    binary_path=VADALOG_WRAPPER_PATH,
    properties=dict(terminationStrategyMode="naiveParsimoniousMode"),
)
tool_registry.register(
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE,
    item_cls=VadalogTool,
    binary_path=VADALOG_WRAPPER_PATH,
    properties=dict(terminationStrategyMode="aggregateParsimoniousMode"),
)
tool_registry.register(
    ToolID.VADALOG_RESUMPTION,
    item_cls=VadalogTool,
    binary_path=VADALOG_WRAPPER_PATH,
    properties=dict(terminationStrategyMode="lightMode"),
)
tool_registry.register(
    ToolID.VADALOG_PARSIMONIOUS_NAIVE_RESUMPTION,
    item_cls=VadalogTool,
    binary_path=VADALOG_WRAPPER_PATH,
    properties=dict(terminationStrategyMode="naiveParsimoniousMode"),
)
tool_registry.register(
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE_RESUMPTION,
    item_cls=VadalogTool,
    binary_path=VADALOG_WRAPPER_PATH,
    properties=dict(terminationStrategyMode="aggregateParsimoniousMode"),
)
tool_registry.register(
    ToolID.DLVE,
    item_cls=DlvTool,
    binary_path=DLVE_WRAPPER_PATH,
)
