# Week #2 Report (Oct 20 – Oct 26)

---

## 1. Progress This Week
- Created project GitHub repository (`332project`).
- Shared team contacts and communication channels.
- Defined main milestones and full project timeline.
- Decided to record weekly progress in Markdown and share via GitHub repository.


---

## 2. Milestones & Schedule

| Milestone | Period | Goal |
|------------|---------|------|
| **#1 Local Sort & Communication** | Oct 27 – Nov 10 | Build architecture, local sort, and Master–Worker communication. |
| **#2 Sampling & Partition** | Nov 11 – Nov 17 | Implement distributed sorting through sampling and partitioning. |
| **#3 Parallel Merge & Fault Tolerance** | Nov 18 – Dec 7 | Add parallel merge, crash recovery, and finalize documentation. |

**Progress Presentation:** Nov 18–19  **Final Submission:** Dec 7

---

## 3. Next Week’s Tasks (Nov 27 – Nov 2)

### Architecture
- [ ] Finalize how the **Master and Workers** work together.  
- [ ] Decide how to split files into **32MB blocks**.  
- [ ] Draw a simple system diagram.

### Communication
- [ ] Set up a basic **connection test** using gRPC.  
- [ ] Check if **multiple Workers** can connect at once.  
- [ ] Move IP/port info to a config file.

### Data Handling
- [ ] Make sample data with **gensort**.  
- [ ] Write a small program to split records into **key (10B)** and **value (90B)**.  
- [ ] Try sorting locally to check if it works.

### Coordination
- [ ] Divide roles (**Master**, **Worker**, **Integration**).  
- [ ] Plan the next meeting.  


---

**Expected by Next Week:**  
Architecture diagram complete, communication tested, local sort verified.
