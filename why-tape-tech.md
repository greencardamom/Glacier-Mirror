<!--

Copyright (c) 2026 greencardamom
https://github.com/greencardamom/Glacier

This documentation is licensed under a Creative Commons Attribution-ShareAlike 4.0 International License.
See LICENSE.md for more details.
-->
# The Cold Truth: Why LTO Tape Still Rules Amazon's Glacier Deep Archivey

Since its inception, Amazon S3 Glacier has been the subject of intense industry "archaeology." While Amazon treats its hardware stacks as a proprietary secret, the laws of physics and economics leave a clear trail. As of 2026, while SSDs dominate the hot tier and HDDs cling to the warm tier, the evidence demonstrates that Glacier Deep Archive is powered by robotic LTO (Linear Tape-Open) libraries.

The early theory that Amazon used "spun-down" hard drives—a project rumored to be codenamed "Pelican"—has hit a wall of diminishing returns. While it's true that a drive consumes zero power when off, the Capital Expenditure (CapEx) is inescapable.

|                     | HDD for hyperscalers | LTO for hyperscalers |
|---------------------|----------------------|----------------------|
| Acquisition Cost    | ~$9–$12 per TB      | ~$3–$5 per TB        |
| Idle Power          | High (or Zero if off)| Zero                 |
| Lifespan            | 5–10 years          | 10-20 years in high usage environment |
| Bit Error Rate      | 1×10^-15            | 1×10^-19 (Higher reliability) |

Even at a massive scale, AWS cannot bypass this reality. If AWS charges $1 per TB/month, and a drive costs $10/TB, they are looking at 10 months just to recover the hardware cost. When you factor in the failure rates of mechanical drives—even those that are rarely spun up—the math simply fails to compete with the density of LTO. This does not include the cost of electricity even when drives are turned off there is circuitry needed to manage the drives, and the electricity to spin up a drive. Drives were not made for this: wear and tear on a drive that is continuously turned on and off, potentially rapidly many times per 24hr, is not healthy. HDDs can actually last longer if never turned off, with stable electric current and temperatures.

In the mid-2010s, Facebook (Meta) experimented with Blu-ray racks for cold storage. It looked promising on paper, but the technology hit a ceiling. Blu-ray capacity didn't scale at the rate of magnetic tape. Picking and placing thousands of small discs is mechanically more "jittery" than handling a single LTO cartridge containing 36TB (native) of data. The consumer decline of physical media meant R&D for high-capacity optical discs dried up, leaving LTO as the only standing "Removable Media" giant.

The strongest evidence for tape is the latency requirement. If Glacier used HDDs, even a spun-down drive could be ready in 30 to 60 seconds. Yet, Deep Archive requires a standard 12 to 48 hours to "thaw". This delay is not arbitrary punitive which would only give competitors room to offer the same service without a "thaw". It is based on a deliberate engineering choice to solve the tape drive wear problem. In a library of 100,000 tapes, there might only be 100 tape drives.

> **The Solution:** AWS doesn't fetch your data when you ask for it. They wait until they have 500 requests for "Tape-Block-A" and "Tape-Block-B."

By batching requests, it minimizes mounting cycles reducing the number of times a robot arm has to move. Data can be retrieved for multiple customers from the same tape, meaning the tape only has to go through a single forward-rewind cycle, instead of one for each customer. It also reduces the wear on the drive head which is the most expensive replaceable part on the drive and subject to incredible friction.

Tape allows you to scale raw capacity (magnetic media) independently of the read/write hardware. It is far more efficient to have one drive head for every 1,000 tapes than to have 1,000 drive heads for 1,000 disks.

In 2026, "Net Zero" commitments are no longer optional for hyperscalers. Tape is the greenest storage medium available. The energy to produce tape is orders of magnitude less than to produce a HDD. Replacing an entire HDD fleet every X years creates massive waste. Tape cartridges lasting 30 years drastically reduces the hardware replacement cycle.

While Amazon may eventually transition to exotic tech like Project Silica (glass storage), the 2026 reality is built on the back of LTO tape drives. There is no other technology currently available that explains the price and service.
