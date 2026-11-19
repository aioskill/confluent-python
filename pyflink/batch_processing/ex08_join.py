"""Demonstrate a keyed stream-stream join with the DataStream API.

Run from the repository root:

    python ex08_join.py

Both streams are keyed by ``customer_id`` and connected. Managed keyed state
stores records until the matching record arrives on the other stream.


The output format is JSON. The prefix printed by Flink identifies the print
sink subtask and is not part of the JSON record.
For example:
  15> {"order_id": "o-1002", "customer": "Bob", "amount": 80}

"""

import json

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedCoProcessFunction
from pyflink.datastream.state import ValueStateDescriptor


# Purpose: demonstrate a keyed stream-stream join with managed keyed state.
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(16)

print("parallelism: ", env.get_parallelism())

order_type = Types.TUPLE([Types.STRING(), Types.STRING(), Types.INT()])
customer_type = Types.TUPLE([Types.STRING(), Types.STRING()])

orders = env.from_collection(
    [
        ("o-1001", "c-1", 120),
        ("o-1002", "c-2", 80),
        ("o-1003", "c-1", 40),
        ("o-1004", "c-3", 200),
    ],
    type_info=order_type,
)

customers = env.from_collection(    [
              ("c-1", "Alice"),
              ("c-2", "Bob"),
              ("c-3", "Carol")
     ],
    type_info=customer_type,
)


class JoinOrderWithCustomer(KeyedCoProcessFunction):
    """
    Join both streams when records with the same customer id arrive.


    process_element1 and process_element2 are symmetric, but handle different input streams
    This symmetry is required because either stream may arrive first.

    For example:
      order arrives → stored
      customer arrives → finds stored order → emits join result

    Or:
      customer arrives → stored
      order arrives → finds stored customer → emits join result

     The implementation is an inner join. Records without a matching counterpart remain in state and produce no output.
     In a real streaming job, add state TTL or cleanup logic to prevent unmatched records from remaining indefinitely.
    """

    def open(self, runtime_context):
        self.orders_state = runtime_context.get_state(
            ValueStateDescriptor(
                "orders",
                Types.LIST(Types.TUPLE([Types.STRING(), Types.STRING(), Types.INT()])),
            )
        )
        self.customers_state = runtime_context.get_state(
            ValueStateDescriptor(
                "customers",
                Types.LIST(Types.TUPLE([Types.STRING(), Types.STRING()])),
            )
        )

    def process_element1(self, order, ctx):
        """
          1. Reads previously received customers.
          2. Emits a joined result for each matching customer.
          3. Stores the order for future customer records.
        """
        customers = self.customers_state.value() or []
        for customer in customers:
            yield json.dumps({
                "order_id": order[0],
                "customer_id": customer[0],
                "customer": customer[1],
                "amount": order[2],
            })
        orders = self.orders_state.value() or []
        self.orders_state.update(orders + [order])

    def process_element2(self, customer, ctx):
        """
          1. Reads previously received orders.
          2. Emits a joined result for each matching order.
          3. Stores the customer for future order records.
        """
        orders = self.orders_state.value() or []
        for order in orders:
            yield json.dumps({
                "order_id": order[0],
                "customer_id": customer[0],
                "customer": customer[1],
                "amount": order[2],
            })
        customers = self.customers_state.value() or []
        self.customers_state.update(customers + [customer])


# A keyed connect sends matching keys to the same downstream task.
joined = (
    orders.key_by(lambda value: value[1], key_type=Types.STRING())
    .connect(customers.key_by(lambda value: value[0], key_type=Types.STRING()))
    .process(JoinOrderWithCustomer(), output_type=Types.STRING())
)
joined.print()

env.execute("Keyed Join")
