# Week #5 Report (Nov 10 – Nov 16)

---

## 1. Last Week Checkpoints
#### 최윤성 (Master Lead)
- [ ] Implement the Master node(proto) to:
  - Accept Worker registration via gRPC. 
  - Receive and log sample keys from Workers.

#### 지예주 (Worker Lead)
- [ ] Implement the Worker node(proto) to:
  - Read a gensort block.
  - Sort records locally (10B key ascending).
  - Send sample keys to the Master.

#### 오지훈 (Integration/Surgeon)
- [X] Prepare presentation slides.
- [ ] Finalize the project structure design.   (85%)
- [X] Support the local sort implementation and interface alignment.

---
## 2. Progress in the week
### Team Progress
1. Created presentation slides
2. Created basic structure & classes in repo
3. Completed basic local sort test with gensort data (binary, ASCII)

---
## 3. Internal Feedback

1. **Task division and execution are not working well.**  
   Even simple assigned tasks are not being finished on time, and it is unclear if everyone is doing their part.

2. **High risk of missing the project deadline.**  
   At this pace, the team needs to understand that **we may not finish the project before the deadline**.

3. **Very weak team communication.**  
   Even though it is **Week 5**, there has been almost no sharing of ideas, discussions or progress updates.

4. It is hard to tell if members are actively involved in the project at all.

---


## 3. Milestones & Schedule(Rearranged)

| Milestone | Period          | Goal | Status                   |
|------------|-----------------|------|--------------------------|
| **#1 Local Sort & Communication** | Oct 27 – Nov 15 | Build architecture, local sort, and Master–Worker communication. | **In Progress<br/>(Delayed)** |
| **#2 Sampling & Partition** | Nov 15 – Nov 22 | Implement distributed sorting through sampling and partitioning. | Upcoming                 |
| **#3 Parallel Merge & Fault Tolerance** | Nov 23 – Dec 7  | Add parallel merge, crash recovery, and finalize documentation. | Later                    |

**Progress Presentation:** Nov 18–20
**Final Submission:** Dec 7  
**Days Remaining Until Progress Presentation:** **1 day**

---

## 4. Goals for Next Week (Week 6: Nov 17 – Nov 23)

### Main Goals
1. Finish Master - Worker Connection.
2. Switch to Phase #2: Sampling & Partitioning.
3. Deliver the progress presentation successfully.

---

### Goals for each Member

#### 최윤성 (Master Lead)
- [ ] Implement Master Partitioning
  - Sample Key Ordering
  - Establishing Partition Plan

#### 지예주 (Worker Lead)
- [ ] Research logging approach

#### 오지훈 (Integration/Surgeon)
- [ ] Deliver the progress presentation successfully.
- [ ] Finish Implementing Master & Worker Communication.
- [ ] Support Implementation in Sampling & Partitioning Phase.