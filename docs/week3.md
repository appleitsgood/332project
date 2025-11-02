# Week #3 Report (Oct 26 – Nov 2)

---

## 1. Last Week Checkpoints
### Architecture
- [X] Finalize how the **Master and Workers** work together.
- [X] Decide how to split files into **32MB blocks**.  
- [ ] Draw a simple system diagram.

### Communication
- [X] Set up a basic **connection test** using gRPC.  
- [X] Check if **multiple Workers** can connect at once.  
- [ ] Move IP/port info to a config file.

### Data Handling
- [X] Make sample data with **gensort**.  
- [ ] Write a small program to split records into **key (10B)** and **value (90B)**.  
- [ ] Try sorting locally to check if it works.

### Coordination
- [X] Divide roles (**Master**, **Worker**, **Integration**).  
- [X] Plan the next meeting.  

---

## 2. Internal Feedback

- Progress has been slower than expected due to individual schedules.  
  Lack of a **Surgeon** caused the team to spend too much time on study and discussion.  
  → The **Surgeon should be appointed as soon as possible**.

- Once the Surgeon is chosen, the team should **follow the Surgeon’s direction** for consistency and efficiency.

- A dedicated **Documentation manager** is needed.  
  Managing notes separately is inefficient — move all documentation to **Notion** for centralized sharing.

- The team’s understanding of the **Sampling Process** is still incomplete.  
  Avoid overanalyzing possible bias; move forward with implementation first.

- With about **two weeks left before the progress presentation**, the team needs to work in a more **focused and concentrated** manner.

---


## 3. Milestones & Schedule

| Milestone | Period | Goal | Status |
|------------|---------|------|--------|
| **#1 Local Sort & Communication** | Oct 27 – Nov 10 | Build architecture, local sort, and Master–Worker communication. | **In Progress** |
| **#2 Sampling & Partition** | Nov 11 – Nov 17 | Implement distributed sorting through sampling and partitioning. | Upcoming |
| **#3 Parallel Merge & Fault Tolerance** | Nov 18 – Dec 7 | Add parallel merge, crash recovery, and finalize documentation. | Later |

**Progress Presentation:** Nov 18–19 (Scheduled for **Tuesday, Nov 18**)  
**Final Submission:** Dec 7  
**Days Remaining Until Progress Presentation:** **16 days**

---

## 4. Next Week Tasks (Week 3: Nov 4 – Nov 10)

### Main Goals
1. **Appoint the Surgeon** and align all development under their direction.  
2. **Complete Master–Worker communication** with gRPC.  
3. **Implement and test local sorting** on Worker side.  
4. **Centralize documentation** on Notion for shared access.  

---

### Team Tasks
- [ ] Select Surgeon by Tuesday(11/4).  
- [ ] Verify gRPC communication works end-to-end (Master ↔ 2 Workers).
- [ ] Move all **team documents** to the shared Notion workspace.

### 👨‍💻 Master Lead
- [ ] Implement Master node that:
  - Accepts Worker registration via gRPC.
  - Receives and logs sample keys from Workers.    
- [ ] Prepare sampling and partition handling design.

### 👩‍💻 Worker Lead
- [ ] Implement Worker node that:
  - Reads a gensort block.  
  - Sorts records locally (key-based ascending).  
  - Sends sample keys to Master.  
- [ ] Send sample keys to Master for testing.

### 🧠 Integration Lead
- [ ] Define gRPC message format and config files.  
- [ ] Lead Surgeon selection and set up Notion documentation space.
- [ ] Move existing reports to Notion and create a shared **project hub** page. 

---


