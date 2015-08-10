"""Simulates running several queries at once but processing results in stages."""

import multiprocessing
import Queue
import time

import pubsubxp


class _StudentPublisher(pubsubxp.Publisher):
  def __init__(self, queue, condition):
    pubsubxp.Publisher.__init__(self, queue)
    self._condition = condition

  def _OpenStream(self):
    time.sleep(3)
    yield {'id': 1, 'name': 'Sue'}
    yield {'id': 6, 'name': 'Jerry'}
    yield {'id': 2, 'name': 'Joe'}
    time.sleep(1)
    yield {'id': 3, 'name': 'Larry'}
    yield {'id': 5, 'name': 'Harry'}
    yield {'id': 4, 'name': 'Bob'}

  def _OnMessage(self, message):
    print 'Published a student', message
    pubsubxp.Publisher._OnMessage(self, message)

  def _OnAfterCloseStream(self):
    # Tell everyone that we've gotten all the students.
    with self._condition:
      self._condition.notify_all()


class _GradePublisher(pubsubxp.Publisher):
  def _OpenStream(self):
    time.sleep(2)
    yield {'student_id': 1, 'grade': 99}
    yield {'student_id': 4, 'grade': 90}
    time.sleep(1)
    yield {'student_id': 6, 'grade': 66}
    yield {'student_id': 5, 'grade': 79}
    time.sleep(1)
    yield {'student_id': 3, 'grade': 100}
    yield {'student_id': 2, 'grade': 80}

  def _OnMessage(self, message):
    print 'Published a grade', message
    pubsubxp.Publisher._OnMessage(self, message)



class _StudentSubscriber(pubsubxp.Subscriber):
  def __init__(self, shared_storage):
    pubsubxp.Subscriber.__init__(self)
    self._shared_storage = shared_storage

  def _OnMessage(self, row):
    self._shared_storage.put(row)


class _GradeSubscriber(pubsubxp.Subscriber):
  def __init__(self, condition, shared_storage):
    pubsubxp.Subscriber.__init__(self)
    self._condition = condition
    self._shared_storage = shared_storage

  def _OnBeforeListeningForMessages(self):
    print "Grade subscriber is waiting for all students' names to be published..."
    with self._condition:
      self._condition.wait()
    print 'All students have been published! Getting their names...'
    name_by_id = {}
    while 1:
      try:
        student = self._shared_storage.get_nowait()
      except Queue.Empty:
        break
      else:
        name_by_id[student['id']] = student['name']
    self._name_by_id = name_by_id
    print 'Student names:', name_by_id
    print 'Listening to new grade messages...'

  def _OnMessage(self, row):
    name = self._name_by_id[row['student_id']]
    print name, 'got a', row['grade']


def main():
  started_at = time.time()
  # A process-safe condition that can be shared by Publishers & Subscribers.
  condition = multiprocessing.Condition()
  # This Queue is thread-safe but not process-safe, so it can only be shared by Subscribers.
  shared_storage = Queue.Queue()

  student_pub = _StudentPublisher(multiprocessing.JoinableQueue(2), condition)
  student_sub = _StudentSubscriber(shared_storage)
  student_sub.Subscribe(student_pub)

  grade_pub = _GradePublisher(multiprocessing.JoinableQueue(2))
  grade_sub = _GradeSubscriber(condition, shared_storage)
  grade_sub.Subscribe(grade_pub)

  grade_sub.start()
  student_sub.start()
  grade_pub.start()
  student_pub.start()

  grade_sub.join()
  print 'Elapsed seconds', time.time() - started_at

if __name__ == '__main__':
  main()
