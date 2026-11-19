"""Demonstrate broadcast state with the DataStream API.

Run from the repository root:

    python ex09_broadcast.py  2>/dev/null

The discount-rule stream is broadcast to every downstream subtask. Each
subtask can then use its local copy to enrich incoming orders.
"""

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import BroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor


# Purpose: demonstrate broadcast state for small reference-data enrichment.
env = StreamExecutionEnvironment.get_execution_environment()

order_type = Types.TUPLE([Types.STRING(), Types.STRING(), Types.INT()])
discount_type = Types.TUPLE([Types.STRING(), Types.FLOAT()])

orders = env.from_collection(
    [
        ("o-1001", "c-1", 120),
        ("o-1002", "c-2", 80),
        ("o-1003", "c-1", 40),
        ("o-1004", "c-3", 200),
    ],
    type_info=order_type,
)

discounts = env.from_collection(
    [("c-1", 0.10),
              ("c-2", 0.20)],
    type_info=discount_type,
)


def discount_descriptor():
    """Create the descriptor without serializing it with the user function."""

    return MapStateDescriptor("customer-discounts", Types.STRING(), Types.FLOAT())


class EnrichWithBroadcastDiscount(BroadcastProcessFunction):
    """Replicate discount rules and use them to enrich each order."""

    def process_broadcast_element(self, discount, ctx):
        ctx.get_broadcast_state(discount_descriptor()).put(discount[0], discount[1])

    def process_element(self, order, ctx):
        discount = ctx.get_broadcast_state(discount_descriptor()).get(order[1])
        discount = discount if discount is not None else 0.0
        yield f"broadcast: order={order[0]}, customer={order[1]}, discount={discount:.2f}"


# Every downstream subtask receives a copy of each discount rule.
enriched = orders.connect(
        discounts.broadcast(discount_descriptor())
    ).process(
    EnrichWithBroadcastDiscount(),
    output_type=Types.STRING(),
)
enriched.print()

env.execute("Broadcast State Enrichment")
