"""Publisher/Subscriber model.

A Publisher publishes messages to its Subscribers via a process-safe Queue.
Each Subscriber thread only subscribes to messages from 1 Publisher.
"""

import collections
import functools
import multiprocessing
import threading
import uuid


# Signal for stopping subscribers.
_STOP = {uuid.uuid4().hex: uuid.uuid4().hex}


def BeforeAndAfter(function):
  """Creates a function that runs its 'OnBefore...' and 'OnAfter...' methods."""
  name = function.func_name[1:]
  @functools.wraps(function)
  def wrapper(self, *args, **kwargs):
    before = getattr(self, '_OnBefore' + name)
    after = getattr(self, '_OnAfter' + name)
    before()
    result = function(self, *args, **kwargs)
    after()
    return result
  return wrapper


class Publisher(multiprocessing.Process):
  def __init__(self, stream):
    multiprocessing.Process.__init__(self)
    self._stream = stream
    self._subscribers = set()

  @property
  def stream(self):
    return self._stream

  @property
  def subscribers(self):
    return self._subscribers

  def AddSubscriber(self, subscriber):
    self._subscribers.add(subscriber)

  def RemoveSubscriber(self, subscriber):
    self._subscribers.discard(subscriber)

  def RemoveAllSubscribers(self):
    self._subscribers.clear()

  @BeforeAndAfter
  def _OpenStream(self):
    """Should return an object with an __iter__ method."""
    raise NotImplemented
  def _OnBeforeOpenStream(self):
    pass
  def _OnAfterOpenStream(self):
    pass

  def _OnStreamError(self, error):
    """Handles an error from getting the next message to stream.

    Should return a truthy value if the error should stop the stream.
    """
    return False

  def _OnMessage(self, message):
    self._stream.put(message)

  @BeforeAndAfter
  def _StartStreaming(self, messages):
    if not isinstance(messages, collections.Iterable):
      raise TypeError('Messages stream must be iterable.')
    while True:
      try:
        message = next(messages)
      except StopIteration:
        break
      except Exception as error:
        if self._OnStreamError(error):
          break
      else:
        self._OnMessage(message)
  def _OnBeforeStartStreaming(self):
    pass
  def _OnAfterStartStreaming(self):
    pass

  @BeforeAndAfter
  def _EndSubscriptions(self):
    for _ in xrange(len(self._subscribers)):
      self._stream.put(_STOP)
  def _OnBeforeEndSubscriptions(self):
    pass
  def _OnAfterEndSubscriptions(self):
    pass

  @BeforeAndAfter
  def _CloseStream(self):
    self._stream.close()
    self._stream.join_thread()
  def _OnBeforeCloseStream(self):
    pass
  def _OnAfterCloseStream(self):
    pass

  def run(self):
    self._StartStreaming(self._OpenStream())
    self._EndSubscriptions()
    self._CloseStream()


class Subscriber(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
    self._has_subscribed_to_a_publisher = False

  @property
  def has_subscribed_to_a_publisher(self):
    return self._has_subscribed_to_a_publisher

  def Subscribe(self, publisher):
    if self._has_subscribed_to_a_publisher:
      raise ValueError('Already subscribed. Cannot subscribe again.')
    publisher.AddSubscriber(self)
    self._stream = publisher.stream
    self._has_subscribed_to_a_publisher = True

  def _OnMessage(self, message):
    raise NotImplemented

  def _OnMessageError(self, error):
    """Handles an error from reading the next streaming message.

    Should return a truthy value if the error should stop the stream.
    """
    return False

  def _OnBeforeListeningForMessages(self):
    pass

  def _OnEndOfMessages(self, final_message):
    pass

  def run(self):
    self._OnBeforeListeningForMessages()
    for message in iter(self._stream.get, _STOP):
      try:
        self._OnMessage(message)
      except Exception as error:
        if self._OnMessageError(error):
          break
      finally:
        self._stream.task_done()
    self._OnEndOfMessages(message)
