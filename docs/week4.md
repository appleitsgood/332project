# Week #3 Report (Oct 26 – Nov 2)

---

## 1. Last Week Checkpoints
### Team Tasks
- [X] Select Surgeon by Tuesday(11/4).
- [X] Verify gRPC communication works end-to-end (Master ↔ 2 Workers).
- [X] Move all **team documents** to the shared Notion workspace.

### 👨‍💻 Master Lead 
- [ ] Implement Master node that:
    - Accepts Worker registration via gRPC.
    - Receives and logs sample keys from Workers.
- [X] Prepare sampling and partition handling design.

### 👩‍💻 Worker Lead 
- [ ] Implement Worker node that:
    - Reads a gensort block.
    - Sorts records locally (key-based ascending).
    - Sends sample keys to Master.
- [ ] Send sample keys to Master for testing.

### 🧠 Integration Lead
- [X] Define gRPC message format and config files.
- [X] Lead Surgeon selection and set up Notion documentation space.
- [X] Move existing reports to Notion and create a shared **project hub** page.

---
## 2. Progress in the week
### Team Progress
1. Appointed Surgeon: 오지훈
2. Created project build repo & basic setup files
3. Completed basic gRPC communication test (Master ↔ Worker)
4. Consolidated project documentation in **Notion**

---
## 3. Internal Feedback

- Progress fell short due to individual schedules again. Everyone needs to commit to a minimum weekly workload to avoid slippage.
- **Progress Presentation** is next week; allocate extra time for presentation prep. Might need to meet face-to-face if necessary.
- Centralize documentation ownership with the surgeon; all members should understand the core design.
- Aim to **finalize the project structure** within this week.

---


## 3. Milestones & Schedule(Rearranged)

| Milestone | Period          | Goal | Status                   |
|------------|-----------------|------|--------------------------|
| **#1 Local Sort & Communication** | Oct 27 – Nov 15 | Build architecture, local sort, and Master–Worker communication. | **In Progress<br/>(Delayed)** |
| **#2 Sampling & Partition** | Nov 15 – Nov 22 | Implement distributed sorting through sampling and partitioning. | Upcoming                 |
| **#3 Parallel Merge & Fault Tolerance** | Nov 23 – Dec 7  | Add parallel merge, crash recovery, and finalize documentation. | Later                    |

**Progress Presentation:** Nov 18–19 (Scheduled for **Tuesday, Nov 18**)  
**Final Submission:** Dec 7  
**Days Remaining Until Progress Presentation:** **9 days**

---

## 4. Goals for Next Week (Week 4: Nov 10 – Nov 17)

### Main Goals
1. Finalize the project structure.
2. Prepare slides for the Progress Presentation.
3. Implement and test **local sorting** on the Worker (define proto where needed).

---

### Goals for each Member

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
- [ ] Prepare presentation slides.
- [ ] Finalize the project structure design.
- [ ] Support the local sort implementation and interface alignment.