import logging
import re
import sys

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, ProcessFunction, KeyedProcessFunction

# Purpose: demonstrate cleaned tokenization, filtering, parallelism, and collection of results.
word_count_data = ["To be, or not to be,--that is the question:--",
                   "Whether 'tis nobler in the mind to suffer",
                   "The slings and arrows of outrageous fortune",
                   "Or to take arms against a sea of troubles,",
                   "And by opposing end them?--To die,--to sleep,--",
                   "No more; and by a sleep to say we end",
                   "The heartache, and the thousand natural shocks",
                   "That flesh is heir to,--'tis a consummation",
                   "Devoutly to be wish'd. To die,--to sleep;--",
                   "To sleep! perchance to dream:--ay, there's the rub;",
                   "For in that sleep of death what dreams may come,",
                   "When we have shuffled off this mortal coil,",
                   "Must give us pause: there's the respect",
                   "That makes calamity of so long life;",
                   "For who would bear the whips and scorns of time,",
                   "The oppressor's wrong, the proud man's contumely,",
                   "The pangs of despis'd love, the law's delay,",
                   "The insolence of office, and the spurns",
                   "That patient merit of the unworthy takes,",
                   "When he himself might his quietus make",
                   "With a bare bodkin? who would these fardels bear,",
                   "To grunt and sweat under a weary life,",
                   "But that the dread of something after death,--",
                   "The undiscover'd country, from whose bourn",
                   "No traveller returns,--puzzles the will,",
                   "And makes us rather bear those ills we have",
                   "Than fly to others that we know not of?",
                   "Thus conscience does make cowards of us all;",
                   "And thus the native hue of resolution",
                   "Is sicklied o'er with the pale cast of thought;",
                   "And enterprises of great pith and moment,",
                   "With this regard, their currents turn awry,",
                   "And lose the name of action.--Soft you now!",
                   "The fair Ophelia!--Nymph, in thy orisons",
                   "Be all my sins remember'd."]

def word_count():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    # write all the data to one file
    env.set_parallelism(4)

    ds = env.from_collection(word_count_data)

    # compute word count
    ds = (ds
            .flat_map(lambda line: re.compile(r"\s+").split(line))
            .filter(lambda r: not re.compile(r"\W+").search(r)) # Remove the non-word characters
            .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
            .key_by(lambda i: i[0])
            .reduce(lambda i, j: (i[0], i[1] + j[1]))
        )
    # DataStream does not have a sort function
    # For sort collect the data or perform sort with a key using processor function
    output = list(ds.execute_and_collect("WordCount", 10000))
    output = sorted(output, key = lambda r: r[1], reverse=True)
    print("Output: ", output)
    print("Job is complete")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    word_count()
